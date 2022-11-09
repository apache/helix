export const HELIX_ENDPOINTS = {
  helix: [
    {
      default: 'http://localhost:8100/admin/v2',
    },
  ],
};
export const SESSION_STORE = undefined;
export const SSL = {
  port: 0,
  keyfile: '',
  certfile: '',
  passfile: '',
  cafiles: [],
};
export const LDAP = {
  uri: 'ldap://example.com',
  base: 'DC=example,DC=com',
  principalSuffix: '@example.com',
  adminGroup: 'admin',
};

/**
 * The url of your Identity Token API.
 * This an API that should expect LDAP credentials
 * and if the LDAP credentials are valid
 * respond with a unique token of some kind.
 */
export const IDENTITY_TOKEN_SOURCE: string | undefined = undefined; // 'www.example.com';

/**
 * Any custom object that you would like
 * to include in the body of the request
 * to your custom identity source.
 */
export const CUSTOM_IDENTITY_TOKEN_REQUEST_BODY: any = {};

/**
 * This is the key that helix-front uses
 * to access the token itself
 * from the custom identity token response
 * sent by your Identity Token API.
 */
export const TOKEN_RESPONSE_KEY: string = 'token';

/**
 * This is the key that helix-front uses
 * to access the token expiration datetime
 */
export const TOKEN_EXPIRATION_KEY: string = 'expires';
