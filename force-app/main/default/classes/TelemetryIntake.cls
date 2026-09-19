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
  }
}
