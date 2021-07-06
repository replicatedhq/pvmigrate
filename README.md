# pvmigrate

pvmigrate allows migrating PVCs between two StorageClasses by creating new PVs, copying over the data, and then changing PVCs to refer to the new PVs.

## examples

To migrate PVs from the 'default' StorageClass to mynewsc:
```bash
pvcmigrate --source-sc default --dest-sc mynewsc
```

## process

In order, it:
1. Validates that both the source and dest StorageClasses exist
1. Finds PVs using the source StorageClass
1. Finds PVCs corresponding to the above PVs
1. Creates new PVCs for each existing PVC, but using the new StorageClass
1. Finds all pods mounting the existing PVCs
1. Finds StatefulSets and Deployments controlling those pods and sets their scale to 0, recording the original scale for later
1. Waits for all pods mounting the existing PVCs to be removed
1. Creates a pod mounting both the original and replacement PVC, which then `rsync`s data between the two (and repeats this for each PVC)
1. Marks all the PVs associated with the original and replacement PVCs as 'retain', so that they will not be deleted when the PVCs are removed
1. Deletes the original PVCs so that their names are available, and removes the association between their PV and the removed PVC
1. Deletes the replacement PVCs so that their PVs are available, and removes the association between their PV and the removed PVC
1. Creates new PVCs with the original names, but associated with the replacement PVs
1. Resets the scales of the affected StatefulSets and Deployments
1. Deletes the original PVs

## known limitations

1. If the migration process is interrupted in the middle, it will not always be resumed properly when rerunning
1. All pods are stopped at once, instead of stopping only the pods whose PVCs are being migrated
1. PVCs are not migrated in parallel
1. Constructs other than StatefulSets and Deployments are not handled (for instance, DaemonSets and Jobs), and will cause pvmigrate to exit with an error
1. Pods not controlled by a StatefulSet or Deployment are not handled, and will cause pvmigrate to exit with an error
1. PVs without associated PVCs are not handled, and will cause pvmigrate to exit with an error
