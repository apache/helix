import { Request } from 'express';
import request from 'request';

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

type AgentOptions = {
  rejectUnauthorized: boolean;
  ca?: string;
};

export type IdentityTokenPostOptions = {
  url: string;
  json: string;
  body: string;
  headers: request.Headers;
  agentOptions: AgentOptions;
};
