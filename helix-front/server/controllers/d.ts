import { Request } from 'express';

export interface HelixUserRequest extends Request {
  session?: HelixSession;
}

interface HelixSession {
  username: string;
  isAdmin: boolean;
}
