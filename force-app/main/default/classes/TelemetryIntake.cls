/*
  "@RestResource" is a class level annotation
  - Inside, each HTTP verb you want to support has its own method, i.e., `@HttpPost; @HttpGet`, etc
  - each method must be 'global static' and not public so that they're accessible for external callers

  the actual incoming request data doesn't arrive as a method param, but comes through as a static context obj
   `RestContext.request` where raw request body as `RestContext.request.requestBody` -> a `Blob` needed `toString()` to become usable text
*/
@RestResource(urlMapping='/TelemetryIntake/*')
global with sharing class TelemetryIntake {
  @HttpPost
  global static void receiveTelemetry(){
    Blob reqBodyBlob = RestContext.request.requestBody;
    String reqBodyStr = reqBodyBlob.toString();

    TelemetryWrapper telem = (TelemetryWrapper)JSON.deserialize(reqBodyStr, TelemetryWrapper.class);

    /*

      The only genuinely new step is the publish call itself: EventBus.publish(), 
      which takes the event instance and returns a Database.SaveResult — the same result type Database.insert() gives you, so checking .isSuccess() should feel familiar.
      
      Construct a new EquipmentTelemetry__e(...), setting both fields from telem's values.
      Publish it with EventBus.publish(...), capturing the result.
    */
    // ✔Construct a Platform Event record
    EquipmentTelemetry__e telemEvent = new EquipmentTelemetry__e(
      Equipment_Id__c = telem.equipment_Id, 
      Engine_Temperature__c = telem.engine_temperature
    );
    // call the EventBus.publish() method. This takes an event instance & returns a Database.SaveResult
    Database.SaveResult result = EventBus.publish(telemEvent);
    // check result.isSuccess(). -> what to do if so? if not? return messages?
    if(result.isSuccess()){
      //do something;
    } else{
      //500: Internal Service Error
      RestContext.response.statusCode = 500;
      // create Log__c object and populated neccessary fields. Should be dynamic for later on.
      Log__c errorLog = new Log__c(
        Category__c = 'Event Publish Failure',
        Message__c = result.getErrors()[0].getMessage(),
        Payload__c = reqBodyStr,
        Source__c = 'ApexClass: TelemetryIntake.receiveTelemetry()'
      );
      insert errorLog;
    }
  }

  private class TelemetryWrapper {
    public String equipment_Id;
    public String engine_temperature;
  }
}
