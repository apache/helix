// stub HelperService for test purpose
export const HelperServiceStub = {
  showError: (message: string) => {},
  showSnackBar: (message: string) => {},
  showConfirmation: (message: string): Promise<boolean> => {
    return new Promise<boolean>(f => f(false));
  }
};
