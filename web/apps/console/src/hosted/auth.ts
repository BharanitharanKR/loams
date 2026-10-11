// Sign-in for the hosted and BYOC console: OIDC Authorization Code with PKCE
// against the configured issuer (LOAMS_AUTH_ISSUER), as a public client.
//
// Tokens live in this module's memory only, never in localStorage or
// sessionStorage. A reload loses them; the console then renews silently
// (`prompt=none` against the issuer's own session cookie) when this tab was
// signed in, and uses the refresh token while the page lives. sessionStorage
// holds only the in-flight PKCE verifier, state and nonce (deleted on return)
// and a "this tab was signed in" marker, none of which grants access.

import * as oauth from 'oauth4webapi';

export interface AuthConfig {
  issuer: string;
  clientId: string;
  scope: string;
}

export interface SignedIn {
  subject: string;
  name?: string;
  email?: string;
}

/** What the callback handling decided. */
export type Callback =
  | { kind: 'none' }
  | { kind: 'signed-in'; returnTo: string }
  /** `prompt=none` found no issuer session: show the sign-in screen. */
  | { kind: 'interaction-required' }
  | { kind: 'error'; message: string };

export interface AuthDeps {
  fetch?: typeof globalThis.fetch;
  storage?: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>;
  /** Navigates the window away (sign-in, sign-out). */
  assign?: (url: string) => void;
  now?: () => number;
}

const PENDING = 'loams.auth.pending';
const MARKER = 'loams.auth.session';
/** Refresh this long before the access token expires. */
const SKEW_MS = 60_000;

interface Pending {
  verifier: string;
  state: string;
  nonce: string;
  returnTo: string;
  silent: boolean;
}

export class OidcAuth {
  readonly #config: AuthConfig;
  readonly #redirectUri: string;
  readonly #fetch: typeof globalThis.fetch;
  readonly #storage: NonNullable<AuthDeps['storage']>;
  readonly #assign: (url: string) => void;
  readonly #now: () => number;
  readonly #client: oauth.Client;
  readonly #insecure: boolean;
  #server?: Promise<oauth.AuthorizationServer>;
  #access?: string;
  #expiresAt = 0;
  #refresh?: string;
  #idToken?: string;
  #user?: SignedIn;
  #refreshing?: Promise<string | undefined>;

  constructor(config: AuthConfig, redirectUri: string, deps: AuthDeps = {}) {
    this.#config = config;
    this.#redirectUri = redirectUri;
    this.#fetch = deps.fetch ?? ((...a) => globalThis.fetch(...a));
    this.#storage = deps.storage ?? globalThis.sessionStorage;
    this.#assign = deps.assign ?? ((url) => globalThis.location.assign(url));
    this.#now = deps.now ?? Date.now;
    this.#client = { client_id: config.clientId };
    // Plain http only for an issuer on this machine (runtime-config.ts allows no other).
    this.#insecure = new URL(config.issuer).protocol === 'http:';
  }

  #options() {
    return { [oauth.customFetch]: this.#fetch, [oauth.allowInsecureRequests]: this.#insecure };
  }

  /** The issuer's metadata, discovered once. */
  server(): Promise<oauth.AuthorizationServer> {
    this.#server ??= (async () => {
      const issuer = new URL(this.#config.issuer);
      const res = await oauth.discoveryRequest(issuer, { ...this.#options(), algorithm: 'oidc' });
      return oauth.processDiscoveryResponse(issuer, res);
    })();
    this.#server.catch(() => {
      this.#server = undefined;
    });
    return this.#server;
  }

  /** Whether this tab signed in before (so a reload may renew silently). */
  wasSignedIn(): boolean {
    return this.#read(MARKER) === '1';
  }

  user(): SignedIn | undefined {
    return this.#user;
  }

  /** Leaves for the issuer's sign-in page; `silent` asks with `prompt=none`. */
  async signIn(returnTo = '', silent = false): Promise<void> {
    const as = await this.server();
    const verifier = oauth.generateRandomCodeVerifier();
    const pending: Pending = {
      verifier,
      state: oauth.generateRandomState(),
      nonce: oauth.generateRandomNonce(),
      returnTo,
      silent,
    };
    this.#storage.setItem(PENDING, JSON.stringify(pending));
    if (!as.authorization_endpoint) throw new Error('The issuer has no authorization endpoint.');
    const url = new URL(as.authorization_endpoint);
    url.searchParams.set('client_id', this.#config.clientId);
    url.searchParams.set('redirect_uri', this.#redirectUri);
    url.searchParams.set('response_type', 'code');
    url.searchParams.set('scope', this.#config.scope);
    url.searchParams.set('code_challenge', await oauth.calculatePKCECodeChallenge(verifier));
    url.searchParams.set('code_challenge_method', 'S256');
    url.searchParams.set('state', pending.state);
    url.searchParams.set('nonce', pending.nonce);
    if (silent) url.searchParams.set('prompt', 'none');
    this.#assign(url.href);
  }

  /**
   * Handles a return from the issuer: `url` is the page's address. Exchanges the code,
   * keeps the tokens in memory and forgets the pending request either way.
   */
  async completeSignIn(url: URL): Promise<Callback> {
    const params = url.searchParams;
    if (!params.has('state') || !(params.has('code') || params.has('error'))) {
      return { kind: 'none' };
    }
    const raw = this.#read(PENDING);
    this.#storage.removeItem(PENDING);
    if (!raw) return { kind: 'error', message: 'The sign-in response has no matching request.' };
    const pending = JSON.parse(raw) as Pending;
    try {
      const as = await this.server();
      let callback: URLSearchParams;
      try {
        callback = oauth.validateAuthResponse(as, this.#client, params, pending.state);
      } catch (e) {
        if (e instanceof oauth.AuthorizationResponseError) {
          const code = e.error;
          if (
            pending.silent &&
            ['login_required', 'interaction_required', 'consent_required'].includes(code)
          ) {
            this.#storage.removeItem(MARKER);
            return { kind: 'interaction-required' };
          }
          return { kind: 'error', message: e.error_description || code };
        }
        throw e;
      }
      const res = await oauth.authorizationCodeGrantRequest(
        as,
        this.#client,
        oauth.None(),
        callback,
        this.#redirectUri,
        pending.verifier,
        this.#options(),
      );
      const tokens = await oauth.processAuthorizationCodeResponse(as, this.#client, res, {
        expectedNonce: pending.nonce,
        requireIdToken: true,
      });
      this.#take(tokens);
      this.#storage.setItem(MARKER, '1');
      return { kind: 'signed-in', returnTo: pending.returnTo };
    } catch (e) {
      return { kind: 'error', message: e instanceof Error ? e.message : String(e) };
    }
  }

  #take(tokens: oauth.TokenEndpointResponse) {
    this.#access = tokens.access_token;
    this.#expiresAt = tokens.expires_in ? this.#now() + tokens.expires_in * 1000 : 0;
    if (tokens.refresh_token) this.#refresh = tokens.refresh_token;
    if (tokens.id_token) this.#idToken = tokens.id_token;
    const claims = oauth.getValidatedIdTokenClaims(tokens);
    if (claims) {
      this.#user = {
        subject: claims.sub,
        name: typeof claims.name === 'string' ? claims.name : undefined,
        email: typeof claims.email === 'string' ? claims.email : undefined,
      };
    }
  }

  /** The access token, refreshed first when it is about to expire; undefined when signed out. */
  async accessToken(): Promise<string | undefined> {
    if (!this.#access) return undefined;
    if (!this.#expiresAt || this.#now() < this.#expiresAt - SKEW_MS) return this.#access;
    if (!this.#refresh) return this.#access;
    this.#refreshing ??= this.#refreshNow().finally(() => {
      this.#refreshing = undefined;
    });
    return this.#refreshing;
  }

  async #refreshNow(): Promise<string | undefined> {
    const refresh = this.#refresh;
    if (!refresh) return this.#access;
    try {
      const as = await this.server();
      const res = await oauth.refreshTokenGrantRequest(
        as,
        this.#client,
        oauth.None(),
        refresh,
        this.#options(),
      );
      this.#take(await oauth.processRefreshTokenResponse(as, this.#client, res));
      return this.#access;
    } catch {
      // A refused refresh is a signed-out tab; the engine answers 401 and the
      // console offers to sign in again.
      this.#forget();
      return undefined;
    }
  }

  signedIn(): boolean {
    return this.#access !== undefined;
  }

  #forget() {
    this.#access = undefined;
    this.#refresh = undefined;
    this.#expiresAt = 0;
    this.#user = undefined;
  }

  /** Forgets the tokens and ends the issuer session when the issuer supports RP logout. */
  async signOut(): Promise<void> {
    const idToken = this.#idToken;
    this.#forget();
    this.#idToken = undefined;
    this.#storage.removeItem(MARKER);
    let end: string | undefined;
    try {
      end = (await this.server()).end_session_endpoint;
    } catch {
      end = undefined;
    }
    if (!end) {
      this.#assign(this.#redirectUri);
      return;
    }
    const url = new URL(end);
    url.searchParams.set('client_id', this.#config.clientId);
    url.searchParams.set('post_logout_redirect_uri', this.#redirectUri);
    if (idToken) url.searchParams.set('id_token_hint', idToken);
    this.#assign(url.href);
  }

  #read(key: string): string | null {
    try {
      return this.#storage.getItem(key);
    } catch {
      return null;
    }
  }
}
