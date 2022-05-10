import { Request } from "express"

export interface HelixUserRequest extends Request {
  session?: HelixSession
}

type HelixSession = {
  username: string;
  isAdmin: boolean;
}