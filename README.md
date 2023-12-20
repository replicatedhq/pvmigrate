# pvmigrate

pvmigrate allows migrating PVCs between two StorageClasses by creating new PVs, copying over the data, and then changing
PVCs to refer to the new PVs.

## Preflight Validation

`pvmigrate` will run preflight migration validation to catch any potential failures prior to the migration.

Currently supported validations are:

- Checking for existence of storage classes
- Checking existing PVC access modes are supported on the destination storage provider

## Examples

To migrate PVs from the 'default' StorageClass to mynewsc:

```bash
pvmigrate --source-sc "default" --dest-sc "mynewsc"
```

To run preflight migration validation without actually running the migration operation:

```bash
pvmigrate --source-sc "source" --dest-sc "destination" --preflight-validation-only
```

## Flags

| Flag                        | Type    | Required | Default          | Description                                                                                        |
|-----------------------------|---------|----------|------------------|----------------------------------------------------------------------------------------------------|
| --source-sc                 | String  | ✓        |                  | storage provider name to migrate from                                                              |
| --dest-sc                   | String  | ✓        |                  | storage provider name to migrate to                                                                |
| --namespace                 | String  |          |                  | only migrate PVCs within this namespace                                                            |
| --rsync-image               | String  |          | eeacms/rsync:2.3 | the image to use to copy PVCs - must have 'rsync' on the path                                      |
| --rsync-flags               | String  |          |                  | A comma-separated list of additional flags to pass to rsync when copying PVCs                      |
| --set-defaults              | Bool    |          | false            | change default storage class from source to dest                                                   |
| --verbose-copy              | Bool    |          | false            | show output from the rsync command used to copy data between PVCs                                  |
| --skip-source-validation    | Bool    |          | false            | migrate from PVCs using a particular StorageClass name, even if that StorageClass does not exist   |
| --preflight-validation-only | Bool    |          | false            | skip the migration and run preflight validation only                                               |
| --skip-preflight-validation | Bool    |          | false            | skip preflight migration validation on the destination storage provider                            |
| --pod-ready-timeout         | Integer |          | 60               | length of time to wait (in seconds) for validation pod(s) to go into Ready phase                   |
| --delete-pv-timeout         | Integer |          | 300              | length of time to wait (in seconds) for backing PV to be removed when the temporary PVC is deleted |

## Annotations

`kurl.sh/pvcmigrate-destinationaccessmode` - Modifies the access mode of the PVC during migration. Valid options are - `[ReadWriteOnce, ReadWriteMany, ReadOnlyMany]`

## Process

In order, it:

1. Validates that both the `source` and `dest` StorageClasses exist
2. Finds PVs using the `source` StorageClass
3. Finds PVCs corresponding to the above PVs
4. Creates new PVCs for each existing PVC
    * Uses the `dest` StorageClass for the new PVCs
    * Uses the access mode set in the annotation: `kurl.sh/pvcmigrate-destinationaccessmode` if specified on a source PVC
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
