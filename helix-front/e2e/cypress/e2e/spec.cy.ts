import { HelixUIPage } from './app.po';

describe('helix-ui App', () => {
  let page: HelixUIPage;

  beforeEach(() => {
    page = new HelixUIPage();
  });

  it('should display message contains Cluster', () => {
    page.navigateTo();
    expect(page.getParagraphText()).contains('Cluster');
  });
});


// describe('My First Test', () => {
//   it('Visits the initial project page', () => {
//     cy.visit('/')
//     cy.contains('Welcome')
//     cy.contains('sandbox app is running!')
//   })
// })
