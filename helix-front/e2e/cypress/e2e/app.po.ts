import { browser, element, by } from 'protractor';

export class HelixUIPage {
  navigateTo() {
    return browser.get('/');
  }

  getParagraphText() {
    return element(by.css('hi-root h1')).getText();
  }
}
