import { Request } from 'express';

export interface HelixUserRequest extends Request {
  session?: HelixSession;
}

interface HelixSession {
  identityToken: any;
  username: string;
  isAdmin: boolean;
}
