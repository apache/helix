package org.apache.helix.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
}
