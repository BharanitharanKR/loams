# Loams PHP SDK (preview)

> **Pre-release (preview).** This SDK is published as a preview of Loams. Its API
> can change between previews without notice: pin the exact version, and expect
> breaking changes until 1.0. Report problems at
> <https://github.com/ostrium-labs/loams/issues>.

## Install

```sh
composer require loams/loams:0.1.0
```

The package is `loams/loams` on Packagist. Composer has no `preview` stability,
so a prerelease such as `v0.1.0-preview.2` is tagged `v0.1.0-alpha.2` there, and
a project whose `minimum-stability` is `stable` must name it exactly (or add
`@alpha`). It needs PHP 8.2 or newer.

Packagist reads the read-only split repository
[`ostrium-labs/loams-php`](https://github.com/ostrium-labs/loams-php), which
`sdk-release.yml` updates from this directory on each release. Send changes here,
to `sdks/php` in `ostrium-labs/loams`, not to the split repository.

## Use

```php
<?php
require 'vendor/autoload.php';

$client = new Loams\Client('https://acme.loams.dev');
```

## Develop

```sh
composer install
vendor/bin/phpunit
```

Licensed under Apache-2.0 (see `LICENSE`).
