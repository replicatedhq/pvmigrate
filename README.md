# pvmigrate

pvmigrate allows migrating PVCs between two StorageClasses by creating new PVs, copying over the data, and then changing
PVCs to refer to the new PVs.

## Examples

To migrate PVs from the 'default' StorageClass to mynewsc:

```bash
pvmigrate --source-sc default --dest-sc mynewsc
```

## Flags

| Flag                     | Type   | Required | Default          | Description                                                                                      |
|--------------------------|--------|----------|------------------|--------------------------------------------------------------------------------------------------|
| --source-sc              | String | ✓        |                  | storage provider name to migrate from                                                            |
| --dest-sc                | String | ✓        |                  | storage provider name to migrate to
| --namespace              | String |          |                  | namespace to find all PVCs                                                               |
| --rsync-image            | String |          | eeacms/rsync:2.3 | the image to use to copy PVCs - must have 'rsync' on the path                                    |
| --set-defaults           | Bool   |          | false            | change default storage class from source to dest                                                 |
| --verbose-copy           | Bool   |          | false            | show output from the rsync command used to copy data between PVCs                                |
| --skip-source-validation | Bool   |          | false            | migrate from PVCs using a particular StorageClass name, even if that StorageClass does not exist |

## Process

In order, it:

1. Validates that both the `source` and `dest` StorageClasses exist
2. Finds PVs using the `source` StorageClass
3. Finds PVCs corresponding to the above PVs
4. Creates new PVCs for each existing PVC, but using the `dest` StorageClass
5. For each PVC:
    * Finds all pods mounting the existing PVC
    * Finds all StatefulSets and Deployments controlling those pods and adds an annotation with the original scale
      before setting that scale to 0
    * Waits for all pods mounting the existing PVC to be removed
6. For each PVC:
    * Creates a pod mounting both the original and replacement PVC which then `rsync`s data between the two
    * Waits for that invocation of `rsync` to complete
7. For each PVC:
    * Marks all the PVs associated with the original and replacement PVCs as 'retain', so that they will not be deleted
      when the PVCs are removed, and adds an annotation to the replacement PV with the original's reclaim policy
    * Deletes the original PVC so that the name is available, and removes the association between the PV and the removed
      PVC
    * Deletes the replacement PVC so that the PV is available, and removes the association between the PV and the
      removed PVC
    * Creates a new PVC with the original name, but associated with the replacement PV
    * Sets the reclaim policy of the replacement PV to be what the original PV was set to
    * Deletes the original PV
8. Resets the scales of the affected StatefulSets and Deployments
9. If `--set-defaults` is set, changes the default StorageClass to `dest`

## Known Limitations

1. If the migration process is interrupted in the middle, it will not always be resumed properly when rerunning. This
   should be rare, and please open an issue if it happens to you
2. All pods are stopped at once, instead of stopping only the pods whose PVCs are being migrated
3. PVCs are not migrated in parallel
4. Constructs other than StatefulSets and Deployments are not handled (for instance, DaemonSets and Jobs), and will
   cause pvmigrate to exit with an error
5. Pods not controlled by anything are not handled, and will cause pvmigrate to exit with an error
6. PVs without associated PVCs are not handled, and will cause pvmigrate to exit with an error
7. PVCs that are only available on one node (or some subset of nodes) may not have their migration pod run on the proper
   node, which would result in the pod never starting and pvmigrate hanging forever
8. If the default StorageClass is `sc3`, migrating from `sc1` to `sc2` with `--set-defaults` will not change the default
   and will return an error.
