package org.apache.helix.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestParticipantHistory {

  @Test
  public void testGetLastTimeInOfflineHistory() {
    ParticipantHistory participantHistory = new ParticipantHistory("testId");
    long currentTimeMillis = System.currentTimeMillis();
    List<String> offlineHistory = new ArrayList<>();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(currentTimeMillis));
    offlineHistory.add(dateTime);
    participantHistory.getRecord()
        .setListField(ParticipantHistory.ConfigProperty.OFFLINE.name(), offlineHistory);

    Assert.assertEquals(participantHistory.getLastTimeInOfflineHistory(), currentTimeMillis);
  }

  @Test
  public void testGetLastTimeInOfflineHistoryNoRecord() {
    ParticipantHistory participantHistory = new ParticipantHistory("testId");

    Assert.assertEquals(participantHistory.getLastTimeInOfflineHistory(), -1);
  }

  @Test
  public void testGetLastTimeInOfflineHistoryWrongFormat() {
    ParticipantHistory participantHistory = new ParticipantHistory("testId");
    List<String> offlineHistory = new ArrayList<>();
    offlineHistory.add("Wrong Format");
    participantHistory.getRecord()
        .setListField(ParticipantHistory.ConfigProperty.OFFLINE.name(), offlineHistory);

    Assert.assertEquals(participantHistory.getLastTimeInOfflineHistory(), -1);
  }

  @Test
  public void testParseSessionHistoryStringToMap() {
    // Test for normal use case
    ParticipantHistory participantHistory = new ParticipantHistory("testId");
    participantHistory.reportOnline("testSessionId", "testVersion");
    String sessionString = participantHistory.getRecord()
        .getListField(ParticipantHistory.ConfigProperty.HISTORY.name()).get(0);
    Map<String, String> sessionMap =
        ParticipantHistory.sessionHistoryStringToMap(sessionString);

    Assert.assertEquals(sessionMap.get(ParticipantHistory.ConfigProperty.SESSION.name()),
        "testSessionId");
    Assert.assertEquals(sessionMap.get(ParticipantHistory.ConfigProperty.VERSION.name()),
        "testVersion");

    // Test for error resistance
    sessionMap = ParticipantHistory
        .sessionHistoryStringToMap("{TEST_FIELD_ONE=X, 12345, TEST_FIELD_TWO=Y=Z}");

    Assert.assertEquals(sessionMap.get("TEST_FIELD_ONE"), "X");
    Assert.assertEquals(sessionMap.get("TEST_FIELD_TWO"), "Y");
  }

  @Test
  public void testGetHistoryTimestampsAsMilliseconds() {
    ParticipantHistory participantHistory = new ParticipantHistory("testId");
    List<String> historyList = new ArrayList<>();
    historyList.add(
        "{DATE=2020-08-27T09:25:39:767, VERSION=1.0.0.61, SESSION=AAABBBCCC, TIME=1598520339767}");
    historyList
        .add("{DATE=2020-08-27T09:25:39:767, VERSION=1.0.0.61, SESSION=AAABBBCCC, TIME=ABCDE}");
    historyList.add("{DATE=2020-08-27T09:25:39:767, VERSION=1.0.0.61, SESSION=AAABBBCCC}");
    participantHistory.getRecord()
        .setListField(ParticipantHistory.ConfigProperty.HISTORY.name(), historyList);

    Assert.assertEquals(participantHistory.getOnlineTimestampsAsMilliseconds(),
        Collections.singletonList(1598520339767L));
  }

  @Test
  public void testGetOfflineTimestampsAsMilliseconds() {
    ParticipantHistory participantHistory = new ParticipantHistory("testId");
    List<String> offlineList = new ArrayList<>();
    long currentTimeMillis = System.currentTimeMillis();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    String dateTime = df.format(new Date(currentTimeMillis));
    offlineList.add(dateTime);
    offlineList.add("WRONG FORMAT");
    participantHistory.getRecord()
        .setListField(ParticipantHistory.ConfigProperty.OFFLINE.name(), offlineList);

    Assert.assertEquals(participantHistory.getOfflineTimestampsAsMilliseconds(),
        Collections.singletonList(currentTimeMillis));
  }
}
