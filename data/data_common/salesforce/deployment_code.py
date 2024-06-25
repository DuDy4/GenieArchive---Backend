trigger_code = """
trigger ContactChangeEventTrigger on Contact (after insert, after update, after delete) {
    System.debug('Trigger fired');
    List<Map<String, Object>> eventMaps = new List<Map<String, Object>>();
    Set<Id> contactIds = new Set<Id>();

    // Collect Contact IDs for querying Account.Name
    if (Trigger.isInsert || Trigger.isUpdate) {
        for (Contact c : Trigger.new) {
            contactIds.add(c.Id);
        }
    } else if (Trigger.isDelete) {
        for (Contact c : Trigger.old) {
            contactIds.add(c.Id);
        }
    }

    // Query for Account.Name
    Map<Id, Contact> contactMap = new Map<Id, Contact>([
        SELECT Id, FirstName, LastName, Email, Title, Account.Name, LinkedInUrl__c
        FROM Contact
        WHERE Id IN :contactIds
    ]);

    // Build event maps with the queried Account.Name
    if (Trigger.isInsert) {
        for (Contact c : Trigger.new) {
            Map<String, Object> eventMap = new Map<String, Object>();
            eventMap.put('ContactId__c', c.Id);
            eventMap.put('ChangeType__c', 'Insert');

            Map<String, Object> contactData = new Map<String, Object>();
            contactData.put('Id', c.Id);
            contactData.put('FirstName', c.FirstName);
            contactData.put('LastName', c.LastName);
            contactData.put('Email', c.Email);
            contactData.put('Title', c.Title);
            contactData.put('AccountName', contactMap.get(c.Id).Account != null ? contactMap.get(c.Id).Account.Name : null);
            contactData.put('LinkedInUrl__c', c.LinkedInUrl__c);

            eventMap.put('ContactData__c', contactData);  // Include the entire contact record
            eventMaps.add(eventMap);
        }
    } else if (Trigger.isUpdate) {
        for (Contact c : Trigger.new) {
            Map<String, Object> eventMap = new Map<String, Object>();
            eventMap.put('ContactId__c', c.Id);
            eventMap.put('ChangeType__c', 'Update');

            Map<String, Object> contactData = new Map<String, Object>();
            contactData.put('Id', c.Id);
            contactData.put('FirstName', c.FirstName);
            contactData.put('LastName', c.LastName);
            contactData.put('Email', c.Email);
            contactData.put('Title', c.Title);
            contactData.put('AccountName', contactMap.get(c.Id).Account != null ? contactMap.get(c.Id).Account.Name : null);
            contactData.put('LinkedInUrl__c', c.LinkedInUrl__c);

            eventMap.put('ContactData__c', contactData);  // Include the entire contact record
            eventMaps.add(eventMap);
        }
    } else if (Trigger.isDelete) {
        for (Contact c : Trigger.old) {
            Map<String, Object> eventMap = new Map<String, Object>();
            eventMap.put('ContactId__c', c.Id);
            eventMap.put('ChangeType__c', 'Delete');
            eventMaps.add(eventMap);
        }
    }

    String eventsJSON = JSON.serialize(eventMaps);
    System.debug('Events JSON: ' + eventsJSON);
    ContactChangeEventHandler.handleEvent(eventsJSON);
}
"""

class_code = """
public class ContactChangeEventHandler {
    @future(callout=true)
    public static void handleEvent(String eventsJSON) {
        System.debug('Future method called with eventsJSON: ' + eventsJSON);

        // Deserialize the JSON string to a List<Object>
        List<Object> eventList = (List<Object>) JSON.deserializeUntyped(eventsJSON);

        for (Object eventObj : eventList) {
            // Cast each event to a Map<String, Object>
            Map<String, Object> event = (Map<String, Object>) eventObj;
            HttpRequest req = new HttpRequest();
            req.setEndpoint('https://parakeet-apt-piranha.ngrok-free.app/v1/salesforce/webhook'); // Replace with your ngrok URL
            req.setMethod('POST');
            req.setHeader('Content-Type', 'application/json');

            // Extract event details and prepare the payload
            String changeType = (String) event.get('ChangeType__c');
            Map<String, Object> payload = new Map<String, Object>();
            payload.put('ContactId__c', event.get('ContactId__c'));
            payload.put('ChangeType__c', changeType);
            payload.put('ContactData__c', event.get('ContactData__c'));

            String jsonBody = JSON.serialize(event);
            req.setBody(jsonBody);
            Http http = new Http();
            try {
                HttpResponse res = http.send(req);
                System.debug('HTTP Response: ' + res.getBody());
                System.debug('HTTP Status Code: ' + res.getStatusCode());
            } catch (Exception e) {
                System.debug('Error sending HTTP request: ' + e.getMessage());
            }
        }
    }
}
"""

metadata_package = f"""
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <types>
        <members>ContactChangeEventTrigger</members>
        <name>ApexTrigger</name>
    </types>
    <types>
        <members>ContactChangeEventHandler</members>
        <name>ApexClass</name>
    </types>
    <version>61.0</version>
</Package>
"""
