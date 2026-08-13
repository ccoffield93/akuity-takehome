This directory provides a simple functional test to exhibit the behavior of the NamespaceClass custom resource and its controller. 

The test script follows a simple pattern of applying YAMLs, checking the contents of the appropriate namespace, and then updating the namespace or namespace class objects on the cluster (as a user would). The controller, running in the background in another terminal, should create or delete resources in the namespace based on NamespaceClass contents. The test steps are as follows:

1. Install the CRD for NamespaceClass to the cluster. 
2. Apply namespace 1 (NS1) to the cluster, with its NamespaceClass label set to use NamespaceClass 1 (NSC1). 
- The matching NamespaceClass is not applied yet. The Controller should see this, and take no action, so no resources will appear in the namespace. 
3. Retrieve the resources in NS1 to prove nothing was created. 
4. Apply NamespaceClass 1 (NSC1). 
- NSC1, at this stage, has a ServiceAccount and NetworkPolicy.
- Because NS1 is labelled to use NSC1, these two resources should be created in NS1. 
5. Retrieve the resources in NS1 to prove that the resources were successfully created, because the NSC controller detects the assignment. 
- This validates that if a NS is created first, then a NSC is created second, the NSC contents are retroactively applied.
6. Apply NamespaceClass 2 (NSC2). Like NSC1, it contains a  ServiceAccount and NetworkPolicy.
7. Apply Namespace 2 (NS2). 
8. Retrieve the NSC2 resources in NS2 to validate that they are created.
- This validates that if a NSC is created first, then a NS is created second, the NSC contents are read in successfully and applied. 
9. Modify NSC1 to remove one of its elements, the ServiceAccount. 
10. Retrieve NS1 resources to see that the ServiceAccount was deleted. 
- This validates that if a NSC is modified to remove resources, the resources on linked NS are cleaned up. 
11. Modify NSC1 to add the ServiceAccount resource back, and an additional ConfigMap resource. 
12. Retrieve NS1 resources to see the ConfigMap and ServiceAccount have been added. 
- This validates that if a NSC is modified to have additional resources, they are added to the linked NS. 
13. Modify NS1 (not NSC1!) to use NSC2 instead. 
14. Retrieve NS1 resources to see that the NSC1 resources, including the newly added ConfigMap, are deleted. 
- This validates that a NS can switch to a new NSC, and the controller will delete/create resources as needed. 
15. Run cleanup script to remove all NS and NSC, as well as the NamespaceClass CRD, so they can all be installed fresh on the next run. 