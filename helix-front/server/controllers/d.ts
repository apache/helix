import { Request } from 'express';
import request from 'request';

export interface HelixRequest extends Request {
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

export type HelixRequestOptions = {
  url: string;
  json: string;
  headers: request.Headers;
  agentOptions: AgentOptions;
  body?: string;
};
