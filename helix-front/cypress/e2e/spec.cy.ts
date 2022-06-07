describe('helix-ui App', () => {
  it('Displays a message that contains Cluster', () => {
    cy.visit('/')
    cy.contains('Cluster')
  })
})
