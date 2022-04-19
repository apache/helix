export type HelixRequest = Request & {
  session: {
    username?: string
    isAdmin?: boolean
    current?: string
  }
}
