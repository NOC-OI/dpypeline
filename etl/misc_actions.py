def create_slurm_cluster(event, data,
                         name: str = "test", n_procs: int | None = None, n_cores: int = 16, memory: str = "64 GB",
                         queue: str = "par-single", walltime: str = "24:00:00", n_scale: int = 1):
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster

    # Create the cluster and client
    cluster = SLURMCluster(name=name, processes=n_procs, cores=n_cores, memory=memory, queue=queue, walltime=walltime)
    client = Client(cluster)
    cluster.scale(n=n_scale)

    return cluster, client


def create_local_cluster(event, data,
                         n_workers: int = 4,
                         threads_per_worker: int = 1,
                         memory: str = "16 GB", processes: bool = False):

    from dask.distributed import Client, LocalCluster

    # Create the cluster
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=threads_per_worker,
                           processes=processes, memory_limit=memory)

    # Create the client
    client = Client(cluster)

    return cluster, client


def empty_stage(event, data, *args, **kwargs):
    return data
