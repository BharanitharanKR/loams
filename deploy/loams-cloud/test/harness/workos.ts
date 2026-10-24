// A recorded fake of the WorkOS pieces the control plane calls, and an AuthKit
// token issuer with its own RSA key and JWKS. Nothing here reaches WorkOS.

import { JwtVerifier, type Jwk } from "../../src/jwt.ts";
import type { TotpEnrolment, WorkOsMfa } from "../../src/workos.ts";

const enc = new TextEncoder();

function b64url(b: Uint8Array | string): string {
  const bytes = typeof b === "string" ? enc.encode(b) : b;
  let s = "";
  for (const x of bytes) s += String.fromCharCode(x);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export const ISSUER = "https://api.workos.com/user_management/client_TEST0000000000";
export const OSTRIUM_ORG = "org_01OSTRIUM00000000000000";

export class TokenIssuer {
  readonly key: CryptoKeyPair;
  readonly kid: string;
  readonly jwk: Jwk;
  private constructor(key: CryptoKeyPair, kid: string, jwk: Jwk) {
    this.key = key;
    this.kid = kid;
    this.jwk = jwk;
  }

  static async create(kid = "key_1"): Promise<TokenIssuer> {
    const key = (await crypto.subtle.generateKey(
      { name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" },
      true,
      ["sign", "verify"],
    )) as CryptoKeyPair;
    const pub = (await crypto.subtle.exportKey("jwk", key.publicKey)) as JsonWebKey;
    return new TokenIssuer(key, kid, { kty: "RSA", kid, alg: "RS256", use: "sig", n: pub.n, e: pub.e });
  }

  async sign(claims: Record<string, unknown>, header: Record<string, unknown> = {}): Promise<string> {
    const h = b64url(JSON.stringify({ alg: "RS256", typ: "JWT", kid: this.kid, ...header }));
    const p = b64url(JSON.stringify(claims));
    const sig = new Uint8Array(await crypto.subtle.sign("RSASSA-PKCS1-v1_5", this.key.privateKey, enc.encode(`${h}.${p}`)));
    return `${h}.${p}.${b64url(sig)}`;
  }

  /** An admin's access token, valid for 5 minutes from `nowMs`. */
  adminToken(nowMs: number, over: Record<string, unknown> = {}): Promise<string> {
    const iat = Math.floor(nowMs / 1000);
    return this.sign({ iss: ISSUER, sub: "user_01ADMIN000000000000000", sid: "session_01ADMIN0000000000", org_id: OSTRIUM_ORG, role: "cloud_admin", roles: ["cloud_admin"], permissions: [], iat, exp: iat + 300, ...over });
  }

  verifier(now: () => number, keys: () => Jwk[] = () => [this.jwk]): JwtVerifier {
    return new JwtVerifier(async () => ({ keys: keys() }), { issuer: ISSUER, now });
  }
}

/** WorkOS's TOTP factors: each factor's current code is `codes.get(factorId)`. */
export class FakeMfa implements WorkOsMfa {
  private n = 0;
  readonly codes = new Map<string, string>();
  readonly challenges = new Map<string, { factorId: string; used: boolean }>();
  readonly calls: string[] = [];

  async enrollTotp(userId: string, issuer: string, user: string): Promise<TotpEnrolment> {
    this.calls.push(`enroll ${userId}`);
    const factorId = `auth_factor_${String(++this.n).padStart(20, "0")}`;
    this.codes.set(factorId, "246810");
    return { factorId, qrCode: "data:image/png;base64,AAAA", secret: "JBSWY3DPEHPK3PXP", uri: `otpauth://totp/${issuer}:${user}?secret=JBSWY3DPEHPK3PXP` };
  }
  async challenge(factorId: string): Promise<string> {
    this.calls.push(`challenge ${factorId}`);
    const id = `auth_challenge_${String(++this.n).padStart(20, "0")}`;
    this.challenges.set(id, { factorId, used: false });
    return id;
  }
  async verify(challengeId: string, code: string): Promise<boolean> {
    this.calls.push(`verify ${challengeId}`);
    const c = this.challenges.get(challengeId);
    if (!c || c.used) return false;
    c.used = true;
    return this.codes.get(c.factorId) === code;
  }
}
