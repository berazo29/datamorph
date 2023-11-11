# Datamorph

Example of ETL (Extract, Transform, Load) application using Spark, SpringBoot and Java.

### Params

- `arg[0]`: Filename
- `arg[1]`: schema

### Activate Profile

#### For Local Development

Activate the `local` profile.

```
SPRING_PROFILES_ACTIVE=local
```

**Note:**

- Files will be output at `target/write` folder.
- It contains `good` and `corrupted` records each in individual directories

#### For Yarn - Cluster

The `default` profile will be loaded.
No need to set anything.
