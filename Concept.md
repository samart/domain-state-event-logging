

Developer Platforms Typically expose an abstraction for developers to work with and self serve in a system. 

This is exemplified well with Kubernetes, whose domain exposed to users are done in the form of Manifests and stanzas for the concepts and objects of the systems.   For example, Pod, Deployment, etc.  

A typical approach is to build a CLI that teh developer uses to process the manifest they write and submit it to the backend (control plane) for validation, persistence, and to advance a workflow (like deploying my app). 

I've recently built multiple platforms that ultiize this concept.   In particular, i developed an apprach to storing System Domain as a series of events in an objector k/v store Like DynamoDb.  

SO a domain object "update" is wrapped in an Event that describes the action. 

The latest event has the latest domain mutation. 

Using DynamoDB single table design, we can have things like a series of related events (multiple related domains saved in a workflow) retrried in one go, for example with a pipeline id, or execution id.

The approach is as follows.

A user updates a YAML Manifest and then runs the cli to apply the change to the system.   example:

platform-cli apply manifest.yaml

```yaml
apiVersion: 
kind: Artifact
meta:
  name: artifact-name
  appId: 09287

spec:
  region: 
    - us-east-1
    - us-east-2
  bundles:
    - name: a1.zip
      type: terraform
      location: http://artifactory.myco.com/lob1/a1.zip
    - name: a2.zip
      type: terraform
      location: http://artifactory.myco.com/lob2/a2.zip


```

when i run my cli against this i may run the following commands

mycli validate
mycli apply

On validate, 

The client (CLI) would send the user created YAML to the backend for Validation.

The payload sent would look like this:


```yaml
apiVersion: 
kind: ArtifactValidateEvent
meta:
  name: artifact-name
  appId: 09287

spec:
  artifact:
    apiVersion: 
    kind: Artifact
    meta:
    name: artifact-name
    appId: 09287

    spec:
    region: 
        - us-east-1
        - us-east-2
    bundles:
        - name: a1.zip
        type: terraform
        location: http://artifactory.myco.com/lob1/a1.zip
        - name: a2.zip
        type: terraform
        location: http://artifactory.myco.com/lob2/a2.zip
    region: 
        - us-east-1
        - us-east-2
    bundles:
        - name: a1.zip
        type: terraform
        location: http://artifactory.myco.com/lob1/a1.zip
        - name: a2.zip
        type: terraform
        location: http://artifactory.myco.com/lob2/a2.zip

```


The response payload might look like:


```yaml
apiVersion: 
kind: ArtifactValidatedEvent
meta:
  name: artifact-name
  appId: 09287

spec:
  validation:
    overall: SUCCCESS
    details:
      validations:
        - name: rule1
          pass: true
        - name: rule2
          pass: true
        - name: rule3
          skipped: true
          

```

Note Validate on the attempt, and Validated on the completion.

Both the Validate and the Validated event are stored in the database, in this case a single DynamoTable (single table design)




Now to apply (which saves and or executes the object) this kind to the system, we can run 

platform-cli apply myfile.yaml 

This would apply the object kind in myfile.yaml, wrapping in Apply and Applied events.  

The client would sent the "present tense" action (in this case 'Apply')

```yaml
apiVersion: 
kind: ArtifactApplyEvent
meta:
  name: artifact-name
  appId: 09287

spec:
  artifact:
    apiVersion: 
    kind: Artifact
    meta:
    name: artifact-name
    appId: 09287

    spec:
    region: 
        - us-east-1
        - us-east-2
    bundles:
        - name: a1.zip
        type: terraform
        location: http://artifactory.myco.com/lob1/a1.zip
        - name: a2.zip
        type: terraform
        location: http://artifactory.myco.com/lob2/a2.zip
    region: 
        - us-east-1
        - us-east-2
    bundles:
        - name: a1.zip
        type: terraform
        location: http://artifactory.myco.com/lob1/a1.zip
        - name: a2.zip
        type: terraform
        location: http://artifactory.myco.com/lob2/a2.zip

```

If there is some error, no applied event is saved.  This means the last saved Applied is the latest state. 

If valid and no errors, whats saved in the db looks like the below 


```yaml
apiVersion: 
kind: ArtifactAppliedEvent
meta:
  name: artifact-name
  appId: 09287

spec:
  artifact:
    apiVersion: 
    kind: Artifact
    meta:
    name: artifact-name
    appId: 09287

    spec:
    region: 
        - us-east-1
        - us-east-2
    bundles:
        - name: a1.zip
        type: terraform
        location: http://artifactory.myco.com/lob1/a1.zip
        - name: a2.zip
        type: terraform
        location: http://artifactory.myco.com/lob2/a2.zip
    region: 
        - us-east-1
        - us-east-2
    bundles:
        - name: a1.zip
        type: terraform
        location: http://artifactory.myco.com/lob1/a1.zip
        - name: a2.zip
        type: terraform
        location: http://artifactory.myco.com/lob2/a2.zip

```


Note Validate on the attempt (Apply), and Validated on the completion (Applied).   In some cases, we may not save the Apply Event, if the Applied is successful, as then that might be a duplicate the data.   that should be optional. 

Both the Validate and the Valoidated event are stored in the database, in this case a single DynamoTable (single table design)


Additionally, we may launch asynchrous (or sync) functions that create AggregateObjects, for example ExecutionStatusAgrregate, that represents the latest status in a series of changes over time to an Execution, enriched with other details, like live status in AWS (resource count, arns, erc), and references to other objects and systems. 





