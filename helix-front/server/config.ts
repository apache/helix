export const HELIX_ENDPOINTS = {
  helix: [{
    default: 'http://localhost:8100/admin/v2'
  }]
};

export const SESSION_STORE = undefined;

export const SSL = {
  port: 0,
  keyfile: '',
  certfile: '',
  passfile: '',
  cafiles: []
};

export function CheckAdmin(username: string, callback: (boolean) => void) {
  callback(username === 'root');
}
