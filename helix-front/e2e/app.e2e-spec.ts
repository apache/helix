import { HelixUIPage } from './app.po';

describe('helix-ui App', () => {
  let page: HelixUIPage;

  beforeEach(() => {
    page = new HelixUIPage();
  });

  it('should display message contains Cluster', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toContain('Cluster');
  });
});
