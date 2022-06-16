describe('helix-ui App', () => {
  it('Displays the page title', () => {
    cy.visit('/');
    cy.get('.helix-title').contains('Helix');
  });
  it('Displays the footer', () => {
    cy.visit('/');
    cy.get('.footer').contains(/©\s\d\d\d\d\sHelix\.\sAll\srights\sreserved\./);
  });
});
