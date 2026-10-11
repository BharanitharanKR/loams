#!/usr/bin/env bash
# One-time settings the blueprints cannot set, then a check that the issuer
# answers as the console and the engine expect. Run from this directory
# after `docker compose up -d`.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a
domain=${AUTH_DOMAIN:-auth.loams.dev}

echo "waiting for the blueprints to apply..."
for _ in $(seq 1 60); do
  if docker compose exec -T worker ak shell -c \
    'from authentik.core.models import Application; import sys; sys.exit(0 if Application.objects.filter(slug="loams-console").exists() else 1)' \
    >/dev/null 2>&1; then
    break
  fi
  sleep 5
done

# A user who could change their email could claim someone else's verified
# address, and the engine admits users by verified email.
docker compose exec -T worker ak shell -c '
from authentik.tenants.models import Tenant
t = Tenant.objects.get(schema_name="public")
t.default_user_change_email = False
t.default_user_change_username = False
t.save()
print("email and username changes: off")
'

echo "checking https://${domain}/.well-known/openid-configuration ..."
discovery=$(curl -fsS "https://${domain}/.well-known/openid-configuration")
issuer=$(printf '%s' "$discovery" | python3 -c 'import json,sys; print(json.load(sys.stdin)["issuer"])')
jwks=$(printf '%s' "$discovery" | python3 -c 'import json,sys; print(json.load(sys.stdin)["jwks_uri"])')
if [ "$issuer" != "https://${domain}/" ]; then
  echo "the issuer is ${issuer}, expected https://${domain}/ (issuer_mode global, Host passed through)" >&2
  exit 1
fi
curl -fsS "$jwks" | python3 -c 'import json,sys; ks=json.load(sys.stdin)["keys"]; assert any(k["kty"]=="RSA" for k in ks), "no RSA key"; print(len(ks), "signing key(s)")'
echo "ok: issuer ${issuer}, client loams-console"
