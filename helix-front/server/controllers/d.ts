import { Request } from 'express';

export interface HelixUserRequest extends Request {
  session?: HelixSession;
}

interface HelixSession {
  // since this token is from a configurable
  // identity source, the format really is
  // `any` from helix-front's point of view.
  identityToken: any;
  username: string;
  isAdmin: boolean;
}
