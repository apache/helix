// The file contents for the current environment will overwrite these during build.
// The build system defaults to the dev environment which uses `environment.ts`, but if you do
// `ng build --env=prod` then `environment.prod.ts` will be used instead.
// The list of which env maps to which file can be found in `angular.json`.
//
// Piwik was renamed to Matomo
// https://github.com/angulartics/angulartics2/commit/241365d099848723ae7c6560f04c7c04dde7e06e
// https://github.com/angulartics/angulartics2/tree/master/src/lib/providers/matomo

export const environment = {
  production: false,
  piwik: {
    url: '//vxu-ld1.linkedin.biz/piwik/',
    id: '3'
  }
};
