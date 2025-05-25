# bug fixes require pre-compilation off since involve redefining another module
#__precompile__(false)

export ElasticManagerWithTunnel
export launchRemoteWorker, launchSlurmWorker

###################
## From master side
###################

using Distributed
using ClusterManagers
using Sockets

verboseLevel = 10

# TODO: need to be able to kill "old" tunnels in public host 
"""
Structure used for a modified ElasticManager that
+ Uses a single socket for communication between workers and master, initiated by the workers
+ "Registers" the master in a public host, through a reverse ssh tunnel. This enables workers to
  connect to this public host, while the master may be behind a NAT and therefore not accessible.

# Constructor parameters:
+ `masterPublicHost::String`: String of the form `[username@]machine` with the host (with optional
        username) that does the tunneling for the local area network (LAN).  
+ `masterPublicPort::Int=40000`: public port for host that does the tunneling
+ `masterLANPort::Int=50000`: internal port for the host that does the tunneling. Ths port is not
    visible from the outside and only needs to be changed if using the same public host to connect
    several (independent) ElasticManagers
+ `cookie::String`: cookie used for the cluster, must be a string of length 16 
    (see Distributed.HDR_COOKIE_LEN)
+ `startTunnel::Bool=true`: when `false` the tunnel is not started (needed if a tunnel has already
    been stated).

# Example: See ClusterUtils/test/test_Distributed.jl

# Operation

1) In the master, `ElasticManagerWithTunnel` is created which triggered:
    [specific to ElasticManagerWithTunnel]
    a) a (reverse) ssh tunnel is created from the public host to the master
            masterPublicHost:masterPublicPort => masterLANAddress:masterLANPort 

    [similar to ElasticManager]

    b) creates the object 
        mgr::ElasticManager
    c) the master starts to listen on 
            masterLANAddress:masterLANPort 
       and accepts connections asynchronously from workers

       when a connection `s::TCPSocket` is received, the master calls
            process_worker_conn(mgr, s)
        which
            1. reads a cookie from s
            2. validates the cookie against the master's cookie 
            3. adds s to the Channel
                mgr.pending::Channel{TCPSocket}

    d) the master launches asynchronously
            process_pending_connections(mgr::ElasticManager)
       that waits on the Channel
            mgr.pending::Channel{TCPSocket}
       which trigger a call to
            Distributed.addprocs(mgr)

    The following functions are also defined:
    e) launch(mgr::ElasticManager, params::Dict, launched::Array, c::Condition
        for each mgr.pending::Channel{TCPSocket}
            create a WorkerConfig() with io equal to the socket
            takes the socket from mgr.pending
            puts the socket into the array `launched'`
        "notifies" c that mgr.pending is has nothing ready
    
    f) manage(mgr::ElasticManager, id::Integer, config::WorkerConfig, op::Symbol)
        if op==:register
            adds config to mgr.active[pid]
        if op==:deregister
            moves item from mgr.active to mgr.terminated

    [specific to ElasticManagerWithTunnel]
    g) replace the "standard" TCP/IP in Distributed.jl/src/managers.jl
          connect(manager::ClusterManager, pid::Int, config::WorkerConfig)
       that does
          1. reads host:port from socket opened by worker
          2. opens a socket to worker with received host:port
          3. returns new socket both for input and outputs

        with just
          1. reads host:port from socket opened by worker
          2. returns socket opened by worker both for input and outputs
          
        + connect() is called by 
           Distributed.jl/src/process_messages.jl/connect_to_peer()
          but this eventually gets triggered by the arrival of a connection from the worker in c) above
    
2) In thw worker, call 
            elastic_worker(cookie, addr="127.0.0.1", port=9009; stdout_to_master=true)
    which
    a) open socket 'c' to master at addr:port
    b) calls 
            start_worker(c, cookie)
       which
            accepts connections asynchronously from master on 'c'
            [originally first creates a new socket, 
             sends port/address to master and waits on this socket]
       wait 60s until key=1 (master) appears in Dict `map_pid_wrkr`
       showing that master connected
"""
struct ElasticManagerWithTunnel <: ClusterManager
    # fields used to "register" master
    masterPublicHost::String
    masterPublicPort::Int
    masterLANAddress::IPv4
    masterLANPort::Int
    processTunnel2master::Base.Process
    # fields when launching workers
    processesLaunchWorkers::Vector{Base.Process}
    # sent by worker and checked by master upon connection in process_worker_conn()
    cookie::String
    # underlying ElasticManager
    em::ElasticManager

    function ElasticManagerWithTunnel(;
        masterPublicHost::String="",
        masterPublicPort::Int=40000,
        masterLANPort::Int=50000,
        cookie::String,
        startTunnel::Bool=true
    )
        @assert length(cookie) == 16 "Cookie must have length 16"

        ## Create tunnel   masterPublicHost:masterPublicPort => masterLANAddress:masterLANPort
        masterLANAddress::IPv4 = ip"127.0.0.1"
        if !isempty(masterPublicHost)
            @printf("registering master on %s:%d => %s:%d\n",
                masterPublicHost, masterPublicPort, masterLANAddress, masterLANPort)

            # get script path
            #(scriptFolder, _) = splitdir(Base.current_project())
            (scriptFolder, _) = splitdir(pathof(ClusterUtils)) # already included src
            launchScript = joinpath(scriptFolder, "ClusterUtils_tunnel2master.sh")

            cmd = `$launchScript -D --public_host $masterPublicHost --port_on_public $masterPublicPort --port_on_lan $masterLANPort`
            @printf("cmd: %s\n", cmd)

            if startTunnel
                # run detached but will be killed when julia ends
                # see https://discourse.julialang.org/t/how-to-run-a-process-in-background-but-still-close-it-on-exit/27231
                process = open(pipeline(cmd))
                #display(tunnel2masterProcess)

                if true
                    # wait a little and show what we got
                    sleep(1)
                    if process_exited(process)
                        for ln in readlines(process)
                            @printf("   %s\n", ln)
                        end
                    else
                        while bytesavailable(process) > 0
                            ln = readline(process)
                            @printf("   %s\n", ln)
                        end
                    end
                end
            end
        end

        ## Create underlying ElasticManager to reuse most of its functions
        em = ElasticManager(;
            cookie,
            addr=masterLANAddress, port=masterLANPort,
            topology=:master_worker)
        emt = new(masterPublicHost, masterPublicPort,
            masterLANAddress, masterLANPort,
            process, Base.Process[],
            cookie, em)
        return emt
    end
end
function Base.display(emt::ElasticManagerWithTunnel)
    @printf("ElasticManagerWithTunnel:\n")
    @printf("   master public = %s:%d => %s:%d\n",
        emt.masterPublicHost, emt.masterPublicPort,
        emt.masterLANAddress, emt.masterLANPort)
    if process_exited(emt.processTunnel2master)
        @printf("      tunnel (exit=%2d) = %s\n",
            emt.processTunnel2master.exitcode, emt.processTunnel2master.cmd)
    else
        @printf("      tunnel (running) = %s\n",
            emt.processTunnel2master.cmd)
    end
    @printf("   cookie = \"%s\"\n", emt.cookie)
    @printf("   launch workers:\n")
    for p in emt.processesLaunchWorkers
        if process_exited(p)
            @printf("      (exit=%2d) %s\n", p.exitcode, p.cmd)
        else
            @printf("      (running) %s\n", p.cmd)
        end
    end
    @printf("ElasticManager:\n")
    @printf("   socket = %s:%d\n", emt.em.sockname[1], emt.em.sockname[2])
    @printf("   topology = %s\n", emt.em.topology)
    print("   active:\n")
    for (k, v) in emt.em.active
        if isnothing(k)
            continue
        end
        @printf("      %3d => %s(%s/%s:%d, ospid=%-8s, env=%s, io=%s)\n",
            k, typeof(v), v.host, v.bind_addr, v.port, v.ospid, v.environ, v.io)
    end
    @printf("   terminated = %s\n", emt.em.terminated)
end

# TODO would be good to define connect(manager::ElasticManagerWithTunnel,...
# TODO but that would likely mean not reusing much of ElasticManager, since by reusing it, connect
# TODO comes to ElasticManager
"""
Changed just to
+ use socket from config.io that comes from the worker connection
+ do not call redirect_worker_output (?) 

# https://github.com/JuliaLang/Distributed.jl/blob/master/src/managers.jl
"""
function Distributed.connect(manager::ElasticManager, pid::Int, config::WorkerConfig)
    @printf("My Distributed.connect(%s)\n", typeof(manager))

    #display(config)

    # master connecting to workers
    if config.io !== nothing
        (bind_addr, port::Int) = Distributed.read_worker_host_port(config.io)
        pub_host = something(config.host, bind_addr)
        config.host = pub_host
        config.port = port
    else
        pub_host = Distributed.notnothing(config.host)
        port = Distributed.notnothing(config.port)
        bind_addr = something(config.bind_addr, pub_host)
    end

    ## CONNECTION FROM MASTER TO WORKERS LOOKS LIKE BLOCK WITH SLURP
    #@show (s, bind_addr) = Distributed.connect_to_worker(bind_addr, port)
    s = config.io

    config.bind_addr = bind_addr

    # write out a subset of the connect_at required for further worker-worker connection setups
    config.connect_at = (bind_addr, port)

    if false
        if config.io !== nothing
            let pid = pid
                Distributed.redirect_worker_output(pid, Distributed.notnothing(config.io))
            end
        end
    end

    #display(config)

    @info @sprintf("My Distributed.connect(%s): got from worker %s:%d (but not using)\n",
        typeof(manager), bind_addr, port)
    (s, s)
end

#################
## launch workers
#################

"""
    launchRemoteWorker( emt,host,project;
                        nWorkers,nThreads=1,localPortOnRemote=60000, launchTunnel=false)

Launch workers in a remote host to join an `ElasticManagerWithTunnel` cluster manager. The connection will use tunneling through a public hosts to which master and workers all connect to.

# Parameters
+ `emt::ElasticManagerWithTunnel`: Cluster manager, created with `ElasticManagerWithTunnel()`
+ `host::String`: host where workers will be launched using `ssh`
+ `project::String`: julia project to be passed to workers using `--project` 
+ `nWorkers=1`: number of workers to be launched
+ `nThreads=1`: number of threads for each worker, to be passed using `--threads`
+ `localPortOnRemote::Int=60000`: local port on workers that is tunneled to the public host
+ `launchTunnel::Bool=false`: when `true` the tunnel is launched on the workers to connect to the
        public host.

# Returns
+ `process::Process`: file descriptor of a pipeline used to communicate with the `ssh` process that launcher
      the workers

!!! note

    In the current version, even if the tunnel is not launched, it is expected to exist (perhaps launched by a previous call to launchRemoteWorker).
"""
function launchRemoteWorker(
    emt::ElasticManagerWithTunnel,
    host::String,
    project::String;
    nWorkers=1,
    nThreads=1,
    localPortOnRemote::Int=60000,
    launchTunnel::Bool=false,
)
    cookie = emt.cookie
    masterPublicHost = emt.masterPublicHost
    masterPublicPort = emt.masterPublicPort

    launchScript = joinpath(splitdir(pathof(ClusterUtils))[1], # already contains src
        "ClusterUtils_launchWorkerRemote.sh")

    tunnel = launchTunnel ? "-t" : "-T"
    remoteCmd = `$launchScript -D $tunnel --cookie $cookie --project $project --nworkers $nWorkers --nthreads $nThreads --public_host $masterPublicHost --port_on_public $masterPublicPort --local_port $localPortOnRemote`
    @printf("remoteCMD: %s\n", remoteCmd)

    sshCmd = `ssh $host $remoteCmd`

    @printf("sshCmd: %s\n", sshCmd)

    # run detached but will be killed when julia ends
    # see https://discourse.julialang.org/t/how-to-run-a-process-in-background-but-still-close-it-on-exit/27231
    @show process = open(pipeline(sshCmd))
    push!(emt.processesLaunchWorkers, process)

    if true
        # wait a little and show what we got
        sleep(2)
        if process_exited(process)
            for ln in readlines(process)
                @printf("   %s\n", ln)
            end
        else
            while bytesavailable(process) > 0
                ln = readline(process)
                @printf("   %s\n", ln)
            end
        end
    end
    return process
end


"""
    launchRemoteWorker( emt,cluster;
                        nWorkers,nThreads=1,localPortOnRemote=60000, launchTunnel=false,
                        dryrun=true, quiet=false)

Launch workers in a remote host to join an `ElasticManagerWithTunnel` cluster manager. The connection will use tunneling through a public hosts to which master and workers all connect to.

# Parameters
+ `emt::ElasticManagerWithTunnel`: Cluster manager, created with `ElasticManagerWithTunnel()`
+ `cluster::ClusterInfo`: cluster where workers will be launched (see `ClusterInfo`)
+ `nWorkers=1`: number of workers to be launched
+ `nThreads=1`: number of threads for each worker, to be passed using `--threads`
+ `localPortOnRemote::Int=60000`: local port on workers that is tunneled to the public host
+ `launchTunnel::Bool=false`: when `true` the tunnel is launched on the workers to connect to the
        public host
+ `dryrun::Bool=true`: when `true` does not actually launch any processes, just shows the commands
        that would have been executed to launch the workers
+ `quiet::Bool=false`: when `false` shows the jobs running on the cluster, using 
        `getRunningJobs(cluster)`

# Returns
+ `out::String`: string returned by the script that launches the worker. For Slurm this string is
        the with the batch job

!!! note

    In the current version, even if the tunnel is not launched, it is expected to exist (perhaps launched by a previous call to launchRemoteWorker).
"""
function launchSlurmWorker(
    emt::ElasticManagerWithTunnel,
    cluster::ClusterInfo;
    nWorkers=1,
    nThreads=1,
    localPortOnRemote::Int=60000,
    launchTunnel::Bool=true,
    dryrun::Bool=true,
    quiet::Bool=false,
)
    cookie = emt.cookie
    masterPublicHost = emt.masterPublicHost
    masterPublicPort = emt.masterPublicPort
    project = cluster.juliaProject

    launchScript = joinpath(cluster.juliaProjectClusterUtils,
        "src", "ClusterUtils_launchWorkerRemote.sh")

    tunnel = launchTunnel ? "-t" : "-T"
    scriptCmd = `$launchScript -D $tunnel --cookie $cookie --project $project --nworkers $nWorkers --nthreads $nThreads --public_host $masterPublicHost --port_on_public $masterPublicPort --local_port $localPortOnRemote`
    @printf("scriptCmd: %s\n", scriptCmd)

    # create folder for job
    jobPath = uniqueOutputFilename("SlurmWorker"; path=cluster.jobsFolderRemote)
    #jobPath = joinpath(cluster.jobsFolderRemote, prefix)
    cmd = sshCmd(cluster, `mkdir -p $jobPath`)
    (_, _, p) = conditionalRun(cmd; dryrun)
    if !isnothing(p) && p.exitcode != 0
        display(p)
        error("unable to create directory \"$jobPath\"")
    end

    # submit job
    remoteCmd = qsubCmd(cluster; jobPath, jobCmd=scriptCmd)
    cmd = sshCmd(cluster, remoteCmd)
    (out::String, err::String, process) = conditionalRun(cmd; dryrun)
    if !dryrun
        push!(emt.processesLaunchWorkers, process)
    end
    if !quiet && !dryrun
        getRunningJobs(cluster; user=cluster.username)
        println()
    end
    if cluster.jobManager == :Slurm
        out = replace(out, r"Submitted batch job " => "")
    end
    return out
end

###################
## From worker side
###################

# TODO: remove parameter stdout_to_master
"""
# https://github.com/JuliaParallel/ClusterManagers.jl/blob/master/src/elastic.jl
"""
function elastic_worker(cookie, addr="127.0.0.1", port=9009; stdout_to_master=true)
    c = connect(addr, port)
    write(c, rpad(cookie, ClusterManagers.HDR_COOKIE_LEN)[1:ClusterManagers.HDR_COOKIE_LEN])
    #stdout_to_master && redirect_stdout(c) # do not redirect to keep socket "clean"
    @printf("My elastic_worker(): send cookie=\"%s\" to %s\n", cookie, c)
    start_worker(c, cookie) # call MyRL.start_worker
    @info "My elastic_worker(): cookie sent"
end

"""
Changed to
+ do not wait for connection from master
+ process_messages in the original socket
# https://github.com/JuliaLang/Distributed.jl/blob/master/src/cluster.jl
"""
function start_worker(
    out::IO,
    cookie::AbstractString=readline(stdin);
    close_stdin::Bool=true,
    stderr_to_stdout::Bool=true
)
    println("My start_worker()")
    Distributed.init_multi()

    if close_stdin # workers will not use it
        redirect_stdin(devnull)
        close(stdin)
    end
    stderr_to_stdout && redirect_stderr(stdout)

    init_worker(cookie)
    interface = IPv4(Distributed.LPROC.bind_addr)
    #dump(Distributed.LPROC)
    #@show out
    if Distributed.LPROC.bind_port == 0
        port_hint = 9000 + (getpid() % 1000)
        @show (port, sock) = listenany(interface, UInt16(port_hint))
        Distributed.LPROC.bind_port = port
    else
        @show sock = listen(interface, Distributed.LPROC.bind_port)
    end
    if false
        # removed
        errormonitor(@async while isopen(sock)
            @show client = accept(sock)
            process_messages(client, client, true)
        end)
    else
        # added
        process_messages(out, out, true)
    end
    print(out, "julia_worker:")  # print header
    print(out, "$(string(Distributed.LPROC.bind_port))#") # print port
    print(out, Distributed.LPROC.bind_addr)
    print(out, '\n')
    flush(out)

    if false
        # removed
        Sockets.nagle(sock, false)
        Sockets.quickack(sock, true)
    else
        # added
        Sockets.nagle(out, false)
        Sockets.quickack(out, true)
    end

    if ccall(:jl_running_on_valgrind, Cint, ()) != 0
        println(out, "PID = $(getpid())")
    end

    try
        # To prevent hanging processes on remote machines, newly launched workers exit if the
        # master process does not connect in time.
        check_master_connect() # MyRL 
        while true
            wait()
        end
    catch err
        print(stderr, "unhandled exception on $(myid()): $(err)\nexiting.\n")
    end

    close(sock)
    exit(0)
end

# TODO: no changes, just verbose
function check_master_connect()
    println("My check_master_connect()")
    timeout = Distributed.worker_timeout() * 1e9
    # If we do not have at least process 1 connect to us within timeout
    # we log an error and exit, unless we're running on valgrind
    if ccall(:jl_running_on_valgrind, Cint, ()) != 0
        return
    end
    #@show Distributed.map_pid_wrkr
    @async begin
        start = time_ns()
        #@show Distributed.map_pid_wrkr
        while !haskey(Distributed.map_pid_wrkr, 1) && (time_ns() - start) < timeout
            sleep(1.0)
            #@show Distributed.map_pid_wrkr
        end

        if !haskey(Distributed.map_pid_wrkr, 1)
            print(stderr, "Master process (id 1) could not connect within $(timeout/1e9) seconds.\nexiting.\n")
            exit(1)
        end
    end
    println("My check_master_connect(): exit with")
    @show Distributed.map_pid_wrkr
end
