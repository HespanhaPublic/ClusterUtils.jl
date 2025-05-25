"""
Set of function that simplify running jobs on remote clusters

Key functions include:
+ ClusterInfo
+ uniqueOutputFilename

+ ElasticManagerWithTunnel, launchRemoteWorker, launchSlurmWorker

+ getRunningJobs, getRunningJob, getFolderJobs, submitJulia
+ downloadJob, downloadAllJobs

Must use
```julia
using Pkg
Pkg.add(path="../PersistentCache",target=:extras)
```
"""
module ClusterUtils

export uniqueOutputFilename, runningInCluster
export ClusterInfo, gitPull

using Printf
using Dates

######################
## Paths anf filenames
######################

"""
Return "unique" filename for output files.

The file's path is selected based on the following rules:
1) when running the system on a cluster as a job using the PBS system, the `path` argument is
   ignored replaced by the directory where qsub is run directory ('PBS_O_WORKDIR')
2) otherwise the `path` argument is used (and assumed absolute or relative to the directory from
   where the script is called from)
    
"""
function uniqueOutputFilename(
    basename::String;
    path::String="",
)
    # get time
    uniqueID::String = Dates.format(now(), "yyyymmdd_HHMMSS_sss")
    filename = @sprintf("%s_%s", basename, uniqueID)
    if runningInPBSCluster()
        path = ENV["PBS_O_WORKDIR"]
    elseif runningInSlurmCluster()
        path = ENV["SLURM_SUBMIT_DIR"]
    end
    return isempty(path::String) ? filename::String : joinpath(path, filename)
end
runningInCluster() = runningInSlurmCluster() || runningInPBSCluster() || runningInSshCluster()
"""
   runningInCluster() 

Return `true` if script is running on a PBS cluster.
"""
runningInPBSCluster() = haskey(ENV, "PBS_O_WORKDIR") && !isempty(ENV["PBS_O_WORKDIR"])
"""
   runningInCluster() 

Return `true` if script is running on a Slurm cluster.
"""
runningInSlurmCluster() = haskey(ENV, "SLURM_SUBMIT_DIR") && !isempty(ENV["SLURM_SUBMIT_DIR"])
"""
   runningInCluster() 

Return `true` if script is running on a cluster.
"""
runningInSshCluster() = haskey(ENV, "SSH_WORKDIR") && !isempty(ENV["SSH_WORKDIR"])
"""
   runningInCluster() 

Return `true` if script is running on a cluster.
"""

######################
## Cluster Information
######################

"""
Structure used to store information used to submit jobs to a cluster.

# Fields:
+ `clusterName::String`: name fo the cluster
+ `jobManager::Symbol`: job management systems, currently supported systems: + `:PBS` - Portable
        Batch System (PBS) + `:Slurm` - Slurm cluster management and job scheduling system
+ `queue::String`: queue to submit jobs (specific to PBS)
+ `jobsFolderRemote::String`: folder where jobs are stored

+ `ssh2submit::String`: ssh destination server used to submit jobs
+ `username::String`: username for the server used to submit jobs

+ `githubFolderRemote::String`: folder where GitHub repositories are maintained in the server
        (absolute or relative to \$HOME)
+ `githubRepositories::String`: vector of uri's GitHub repositories kept at the server, as accepted
        by the `git clone` command

+ `juliaExec::String`: path to julia's executable (for PBS) or script (for Slurm) in the server
  (absolute or relative to \$HOME)
+ `juliaProject`: path to julia's project in the server (absolute or relative to \$HOME)
"""
mutable struct ClusterInfo
    clusterName::String
    jobManager::Symbol
    queue::String
    jobsFolderRemote::String
    jobsFolderLocal::String
    persistentCachePrefixRemote::String
    persistentCachePrefixLocal::String
    # ssh fields
    ssh2submit::String
    username::String
    # GitHub fields
    githubFolderRemote::String
    githubRepositories::Vector{String}
    # julia fields
    juliaExec::String
    juliaProject::String
    juliaProjectClusterUtils::String
    ClusterInfo(;
        clusterName::String,
        jobManager::Symbol,
        queue::String="",
        jobsFolderRemote::String,
        jobsFolderLocal::String,
        persistentCachePrefixRemote::String=joinpath(jobsFolderRemote, "persistent_cache/"),
        persistentCachePrefixLocal::String=joinpath(jobsFolderLocal, "persistent_cache/"),
        ssh2submit::String,
        username::String,
        githubFolderRemote::String,
        githubRepositories::Vector{String}=String[],
        juliaProject::String,
        juliaProjectClusterUtils::String,
        juliaExec::String=joinpath(juliaProjectClusterUtils, "src", "julia.sh"),
    ) = new(clusterName, jobManager, queue,
        jobsFolderRemote, jobsFolderLocal,
        persistentCachePrefixRemote, persistentCachePrefixLocal,
        ssh2submit, username,
        githubFolderRemote, githubRepositories,
        juliaExec, juliaProject, juliaProjectClusterUtils)
end

####################################
## Create and execute commands (Cmd)
####################################

"""Return `ssh` command to execute a command at the cluster server used to submit jobs"""
sshCmd(cluster::ClusterInfo, command::Cmd; background::Bool=false) =
    background ? `ssh -f -l $(cluster.username) $(cluster.ssh2submit) $command` : `ssh -l $(cluster.username) $(cluster.ssh2submit) $command`

"""Return `scp` command to copy a local file to the cluster server used to submit jobs"""
scpCmd(cluster::ClusterInfo, localFile::String, remoteFile::String) =
    `scp -O $localFile $(cluster.username)@$(cluster.ssh2submit):$remoteFile`
# -O = Use the legacy SCP protocol for file transfers instead of the SFTP protocol.

"""Return command to submit a job request in the cluster"""
function qsubCmd(
    cluster::ClusterInfo;
    jobPath::String,
    jobCmd::Cmd,
    nNodes::Int=1,
    exclusive::Bool=true,
)
    if cluster.jobManager == :PBS
        cmd = `\( cd "$jobPath" \&\& qsub -q $(cluster.queue) -o "." -e "." -- $jobCmd \)`
    elseif cluster.jobManager == :Slurm
        if exclusive
            cmd = `\( cd "$jobPath" \&\& sbatch --exclusive --nodes=$nNodes -o "./slurm-%j.out" -e "./slurm-%j.err" -- $jobCmd \)`
        else
            cmd = `\( cd "$jobPath" \&\& sbatch --nodes=$nNodes -o "./slurm-%j.out" -e "./slurm-%j.err" -- $jobCmd \)`
        end
    elseif cluster.jobManager == :ssh
        cmd = `\( cd "$jobPath" \&\& export SSH_WORKDIR="$jobPath" \&\& $jobCmd \> ./job.out 2\> ./job.err \)`
    else
        error("unknown job manager $(cluster.jobManager)")
    end
    return cmd
end

"""Return the command needed to call a julia script"""
function juliaCmd(
    cluster::ClusterInfo;
    project::String,
    script::String,
    scriptInProject=true,
    juliaParameters="",
    scriptParameters="",
)
    function parameters2vector(parameters)
        if isa(parameters, AbstractString)
            parameters = split(parameters)
        end
        if isa(parameters, Vector)
            parameters = convert(Vector{String}, parameters)
        end
        return parameters
    end

    juliaParameters = parameters2vector(juliaParameters)
    scriptParameters = parameters2vector(scriptParameters)

    if !isa(scriptParameters, Cmd)
        cars = Cmd(scriptParameters)
    end
    if scriptInProject
        script = joinpath(cluster.juliaProject, script)
    end
    return `$(cluster.juliaExec) --project=$project $juliaParameters $script $scriptParameters`
end

"""Call julia's run() with options to echo the command or just try a "dryrun" """
function conditionalRun(
    cmd::Cmd;
    quiet::Bool=false,
    dryrun::Bool=true,
    background::Bool=false,
)
    if dryrun
        @printf("%s [dry run]\n", cmd)
        return (out="", err="", rc=nothing)
    end


    if background
        !quiet && @printf("%s [background executing]\n", cmd)
        rc = run(cmd)
        sout = ""
        serr = ""
    else
        !quiet && @printf("%s [executing]\n", cmd)
        # from https://discourse.julialang.org/t/collecting-all-output-from-shell-commands/15592/2
        out = Pipe()
        err = Pipe()
        rc = run(pipeline(ignorestatus(cmd), stdout=out, stderr=err))
        close(out.in)
        close(err.in)

        # supposed to avoid deadlocks, but does not work
        #sout = @async String(readchomp($out))
        #serr = @async String(readchomp($err))
        #return (out=wait(sout), err=wait(serr), rc=rc)

        sout = String(readchomp(out))
        serr = String(readchomp(err))
        !quiet && !isempty(serr) && println("stderr:\n", serr)
    end
    return (out=sout, err=serr, rc=rc)
end

##########################
## Call Git in the cluster
##########################

"""Do a `git pull` to all the repositories in ClusterInfo"""
function gitPull(cluster::ClusterInfo; dryrun::Bool=false)
    for rep in cluster.githubRepositories
        (repName, ext) = splitext(basename(rep))
        repPath = joinpath(cluster.githubFolderRemote, repName)

        # check if path exists
        cmd = sshCmd(cluster, `test -d $(repPath)`)
        (out, err, rc) = conditionalRun(cmd; dryrun)
        if rc.exitcode != 0
            @error("repository $(repPath) does not exists, doing a clone")
            (repParent, repFolder) = splitdir(repPath)
            cmd = sshCmd(cluster, `\( cd $(repParent) \; git clone $rep $repFolder \)`)
            conditionalRun(cmd; dryrun)
            continue
        end

        # git pull
        cmd = sshCmd(cluster, `\( cd $(repPath) \; git pull \)`)
        conditionalRun(cmd; dryrun)
    end
    return nothing
end

include(raw"Submit.jl")

include(raw"Distributed.jl")

end
