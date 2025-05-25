# ClusterUtils

This package provide a set of tools to streamline the following operations:
+ Launching julia scrips on a remote hosts using `ssh` 

+ Launching julia scripts on a computer clusters using [Slurm](https://slurm.schedmd.com) (well
  tested) or [PBS](https://www.openpbs.org) (less tested).

3) Using `Distributed.jl` with workers running on a cluster. 
   
   This functionality builds upon
   [ClusterManagers](https://github.com/JuliaParallel/ClusterManagers.jl/tree/master), but uses
   ssh-tunneling to enable communication between the cluster workers and a local computer that does
   not have a public IP addressed (e.g., in a private network with NAT).

> [!Warning] 
> Running `Distributed.jl` with workers on a cluster has not been well tested and should be viewed
> as experimental.


# Requirements

## For remote server operation

To launch jobs on a remote server, you need:

1) `ssh` installed on the local and the remote server

2) Password-free `ssh` authentication enabling the local computer to connect to the remote server. 

   Under unix/linux, this is typically accomplished by 
   a) first creating a public-private key pair in the local computer, typically using `ssh-keygen` 
   b) and then copying the public key to the server, typically using `ssh-copy-id`

   For more details see, e.g., (https://help.ubuntu.com/community/SSH/OpenSSH/Keys)

3) Optionally, you may want to set your `ssh` configuration to allow for automatic re-use of
   connections. This is done by adding something like the following to the `.ssh/config` file in the
   local computer and the remove server:

    ```.ssh/config
    # automatically re-use connection: -M for 1st and -S for subsequent
    ControlMaster auto
    ControlPath ~/.ssh/ssh_mux_%h_%p_%r
    ```

4) `juliaup` installed on the remote server with the julia executable at `~/.juliaup/bin/julia`

5) A julia project on the remote server with the `ClusterUtils` package installed. The scripts
   launched on the cluster will run under this project.


## For cluster operation

To launch jobs on a cluster, you need to meet the same requirements as for server operation, except that by
"remote server", we mean the host where jobs are submitted (using either `Slurm` or `PBS`).

## For running `Distributed.jl` with workers running on a cluster  [experimental]

To user `Distributed.jl` with workers running on a cluster, you need to meet the same requirements
as for cluster operation, as well as access to a "public-host" (meaning a host that can be resolved
by DNS) and

6) `ssh` installed on the public host, supporting `ssh` tunneling
   
7) Password-free `ssh` authentication from the local computer to the public host 
   
8) Password-free `ssh` authentication from the cluster workers to a public host

This will enable ssh-tunneling between the local computer and cluster workers; even when the local
computer and the cluster workers do not have persistent IP addresses.

# Usage

The first step is to use `ClusterUtils` to create a structure in the local computer with the
relevant information about the remote server or cluster.

```julia
using ClusterUtils

username = "myself"               # username in the cluster
homeRemote = "/homes/$username"   # home directory in the cluster file system
homeLocal = ENV["HOME"]           # home in local host

cluster = ClusterInfo(;
    # cluster name (somewhat arbitrary)
    clusterName="My favorite cluster",
    # type of job manager (either :ssh, :Slurm, or :PBS)
    jobManager=:Slurm,                     
    # username in the cluster
    username,                  
    # remote server (for :ssh) or host used to submit jobs (for :Slurm or :PBS)
    ssh2submit="mycluster.net",
    # remote file-system path of the julia project under which the script will run
    juliaProject=joinpath(homeRemote, "storage/GitHub/projects/MyProject"),
    # remote file-system path of the ClusterUtils repository
    juliaProjectClusterUtils=joinpath(homeRemote, "storage/GitHub/projects/ClusterUtils"),
    # remote file-system path where job folders will be created
    jobsFolderRemote=joinpath(homeRemote, "storage/jobs"),
    # local file-system path that will be rsync'ed with `jobsFolderRemote`
    jobsFolderLocal=joinpath(homeLocal, "clusters/jobs"),
)
```

## Submitting jobs to a remote server

To submit jobs to a remote server, we need:

1) The `ClusterInfo` structure must be created with `cluster.jobManager = :ssh`
   
2) The script must exist in the remote file system under the project in `cluster.juliaProject` 
   The precise (relative) path to the script is provided when the script is launched

   The option `sendScript=true` permits a local script to be copied to the folder and executed there.

3) Just before being executed, a folder is created in `cluster.jobsFolderRemote` and the script is
   executed from within this folder. The script's `stdout` and `stderr` will be stored in this
   folder.

4) If the script creates files, these files should be created in the scripts current folder. This
   will make sure that it is possible to run multiple copies of the same script concurrently, without
   collisions.

Once all this is set, launching the script remotely can be done with:

```julia
submitJulia(cluster;
            script="test/test_SARS.jl",   # path relative to `cluster.juliaProject`
            dryrun=false)
```

Setting `dryrun=true` will not launch the script, and instead displays all the command that would be
executed to accomplish it.

To execute a script that exists locally, we can use

```julia
submitJulia(cluster;
            script="test/test_SARS.jl",   # path relative to local folder
            sendScript=true,
            dryrun=false)
```

This first copies the local script "test/test_SARS.jl" to the server and then launches it.

### Retrieving results

Once the script completes, we get `stdout`, `stderr`, and any files generated by the script using

```julia
downloadAllJobs(cluster, "test_SARS_*"; dryrun=false)
```


## Submitting jobs to a Slurm or PBS cluster

To submit jobs to a remote cluster, we need:

1) The `ClusterInfo` structure must be created with 
   + `cluster.jobManager = :Slurm` if the cluster using [Slurm](https://slurm.schedmd.com) as the job manager
   + `cluster.jobManager = :PBS` if the cluster using [PBS](https://www.openpbs.org) as the job manager

2) The script must exist in the remote file system under the project in `cluster.juliaProject` 
   The precise (relative) path to the script is provided when the script is launched

   The option `sendScript=true` permits a local script to be copied to the folder and executed there.

3) Just before being executed, a folder is created in `cluster.jobsFolderRemote` and the script is
   executed from within this folder. The script's `stdout` and `stderr` will be stored in this
   folder.

4) If the script creates files, these files should be created in the scripts current folder. This
   will make sure that it is possible to run multiple copies of the same script concurrently, without
   collisions.

Once all this is set, launching the script remotely can be done with:

```julia
submitJulia(cluster;
            script="test/test_SARS.jl",   # path relative to `cluster.juliaProject`
            dryrun=false)
```

Setting `dryrun=true` will not launch the script, and instead displays all the command that would be
executed to accomplish it.

To execute a script that exists locally, we can use

```julia
submitJulia(cluster;
            script="test/test_SARS.jl",   # path relative to local folder
            sendScript=true,
            dryrun=false)
```

This first copies the local script "test/test_SARS.jl" to the server and then launches it.

### Checking remote jobs

For Slurm and PBS clusters, we can get a list of all jobs currently running in the cluster using

```julia
dict = getRunningJobs(cluster; user=cluster.username)
display(dict)
```

and we can get information about all jobs in the servers that already completed using

```julia
jobsData = getFolderJobs(cluster; wildcard="test*", quiet=false)
display(jobsData)
```

### Retrieving results

Once the script completes, we get `stdout`, `stderr`, and any files generated by the script using

```julia
downloadAllJobs(cluster, "test_SARS_*"; dryrun=false)
```

For more information, see
```julia
?getRunningJobs
?getRunningJob
?getFolderJobs
?submitJulia
?downloadJob
?downloadAllJobs
```

## Using `Distributed.jl` with workers running in a cluster [experimental]

Create an cluster manager to launch the workers using

```julia
emt = ElasticManagerWithTunnel(;
    masterPublicHost="mypublichost.net",
    cookie="1234567890123456",      
)
```

where 
+ `mypublichost.net` is a public host that will be used to tunnel communication between a (local)
  master and (remote) workers running in a cluster. This host simply needs to have an `ssh` server
  that accepts connections from the master and the servers (typically will have their public keys in
  `.ssh/authorized_keys`)
+ `"1234567890123456"` is the "cookie" used to identify the group of workers for `Distributed.jl`

Start workers in the cluster

```julia
nWorkers = 4
nThreads = 6
@time out = ClusterUtils.launchSlurmWorker(emt,
        cluster;
        launchTunnel=true,
        nWorkers,
        nThreads,
        dryrun=false,
)
```

You may want to wait for the workers to start with

```julia
begin
while length(workers()) < nWorkers
    print('.')
    sleep(2)
end
println("all workers started")
display(emt)
end
```

The following example runs a computationally intensive task in each of the workers:

```julia
using Distributed

@everywhere using Printf
@everywhere using LinearAlgebra

# function that computes statistics of smaller singular value of a matrix
@everywhere function longTask(N; nEpisodes)
    nThreads = Threads.nthreads()
    nBLASthreads = BLAS.get_num_threads()
    t0 = time()
    sm = 0.0
    sm2 = 0.0
    for episode = 1:nEpisodes
        A = randn(Float64, N, N)
        s = svdvals(A)
        s0 = minimum(s)
        sm += s0
        sm2 += s0^2
    end
    mn = sm / nEpisodes
    sd = sqrt(sm2 / N - mn^2)
    dt = time() - t0
    @printf("longTask: mean=%10.5f, std=%10.5f [%.3f sec]\n", mn, sd, dt)
    return (; mean=mn, std=sd, nThreads, nBLASthreads, dt)
end

# launch one task in each node
@assert length(workers()) > 1
N = 200
rcs = []
for worker in workers()
    nEpisodes = 4000 + worker
    @time "remotecall" rc = remotecall(longTask, worker, N; nEpisodes)
    push!(rcs, rc)
end

# fetch results
for rc in rcs
    @time "fetch" result = fetch(rc)
    display(result)
end
```

The following example does the same but uses
[tCache](https://github.com/HespanhaPublic/PersistentCache.jl) for persistent memoization.
   This requires the [PersistentCache](https://github.com/HespanhaPublic/PersistentCache.jl) package
   to be part of the remote host's julia project


```julia
using Distributed

@everywhere using Printf
@everywhere using LinearAlgebra
@everywhere using PersistentCache

# function that computes statistics of smaller singular value of a matrix
@everywhere function longTask(N; nEpisodes)
    nThreads = Threads.nthreads()
    nBLASthreads = BLAS.get_num_threads()
    t0 = time()
    sm = 0.0
    sm2 = 0.0
    for episode = 1:nEpisodes
        A = randn(Float64, N, N)
        s = svdvals(A)
        s0 = minimum(s)
        sm += s0
        sm2 += s0^2
    end
    mn = sm / nEpisodes
    sd = sqrt(sm2 / N - mn^2)
    dt = time() - t0
    @printf("longTask: mean=%10.5f, std=%10.5f [%.3f sec]\n", mn, sd, dt)
    return (; mean=mn, std=sd, nThreads, nBLASthreads, dt)
end

# version of the function that uses memoization using Persistent Cache
@everywhere function cached_longTask(rCache, N; nEpisodes)
    rc = @pcacheref rCache longTask(N; nEpisodes)
    return value(rc)
end

## % Run multiple tasks, one per node with persistent cache
@assert length(workers()) > 1
N = 200
rcs = []
for worker in workers()
    nEpisodes = 4000 + worker
    @time "remotecall" rc = remotecall(cached_longTask, worker, rCache, N; nEpisodes)
    push!(rcs, rc)
end

for rc in rcs
    @time "fetch" result = fetch(rc)
    display(result)
end
```

Once we are done with the workers, they can be killed using

```julia
rmprocs(workers())
```

For more information, see

```julia
?ElasticManagerWithTunnel
?launchRemoteWorker
?launchSlurmWorker
```

# Limitations

1) This package has mostly been tested with the local host running unix/OSX and the remote
   server/cluster running linux

2) The use of `Distributed.jl` with workers running on a cluster is experimental and not very well
   tested. Mostly, because I ended up finding it much less useful than running scripts remotely.