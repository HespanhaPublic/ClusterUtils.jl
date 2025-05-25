export getRunningJobs, getRunningJob, getFolderJobs, submitJulia
export downloadJob, downloadAllJobs

##################
## Jobs submission
##################

"""
Submit julia job

# Parameters:
+ `cluster::ClusterInfo`: structure with the cluster information
+ `script::String`: script to execute, including its path relative to the julia's project folder
+ `juliaParameters::String=""`: parameters to pass to julia
+ `scriptParameters::String=""`: parameters to pass to the script
+ `scriptInProject::Bool=true`: when `true` assumes that the script is part of the project in
        `cluster` and its path is relative to the project home folder.
+ `sendScript::Bool=false`: when `true` sends the script to the server.

+ `project::String=cluster.juliaProject`: path to julia's project folder, relative to \$HOME

+ `nNodes::Int=1`: number of nodes to reserve
+ `exclusive::Bool=true`: when `true` nodes are not shared

+ `prefix::String=splitext(basename(script))[1]`: prefix for the job folder in the server
+ `dryrun::Bool=true`: when `true` commands are not executed, just printed
+ `quiet::Bool=false`: display progress

# Returns:
+ `out::String`: job id returned by qsub
"""
function submitJulia(
    cluster::ClusterInfo;
    script::String,
    juliaParameters::String="",
    scriptParameters::String="",
    sendScript::Bool=false,
    scriptInProject::Bool=true,
    project::String=cluster.juliaProject,
    nNodes::Int=1,
    exclusive::Bool=true,
    prefix::String=splitext(basename(script))[1],
    dryrun::Bool=true,
    quiet::Bool=false,
)
    # create folder for job
    jobPath = uniqueOutputFilename(prefix; path=cluster.jobsFolderRemote)
    #jobPath = joinpath(cluster.jobsFolderRemote, prefix)
    cmd = sshCmd(cluster, `mkdir -p $jobPath`)
    (_, _, p) = conditionalRun(cmd; dryrun)
    if !isnothing(p) && p.exitcode != 0
        display(p)
        error("unable to create directory \"$jobPath\"")
    end

    if sendScript
        @assert !scriptInProject "`sendScript==true` not compatible with `scriptInProject==true`"
        # send script to server
        (_, file) = splitdir(script)
        remoteScript = joinpath(jobPath, file)
        cmd = scpCmd(cluster, script, remoteScript)
        (_, _, p) = conditionalRun(cmd; dryrun)
        if !isnothing(p) && p.exitcode != 0
            display(p)
            error("unable to copy script \"$remoteScript\"")
        end
        script = remoteScript
        scriptInProject = false
    end

    # submit job
    jobCmd = juliaCmd(cluster; project, script, scriptInProject, juliaParameters, scriptParameters)
    remoteCmd = qsubCmd(cluster; jobPath, jobCmd, nNodes, exclusive)
    cmd = sshCmd(cluster, remoteCmd, background= (cluster.jobManager == :ssh))
    (out::String, err::String, p) = conditionalRun(cmd; dryrun, background= (cluster.jobManager == :ssh))
    if !quiet && !dryrun
        getRunningJobs(cluster; user=cluster.username)
        println()
    end
    if cluster.jobManager == :Slurm
        out = replace(out, r"Submitted batch job " => "")
    end
    return out
end

"""
    getRunningJobs(cluster;user,quiet)

Displays/returns all running/historic jobs:

# Parameters:
+ `cluster::ClusterInfo`: structure with the cluster information
+ `user::String=""`: when empty returns jobs for all users, otherwise just for the given user
+ `quiet::Bool=false`: display information, rather than just returning it

# Returns:
+ `jobsData::Dict{String,Dict{String,String}}`: dictionary with one key per jobs field, whose values
        are dictionaries that describe the corresponding job.
"""
function getRunningJobs(
    cluster::ClusterInfo;
    user::String="",
    quiet::Bool=false
)
    if cluster.jobManager == :PBS
        if isempty(user)
            cmd = sshCmd(cluster, `qstat`)
        else
            cmd = sshCmd(cluster, `qstat -u $user`)
        end
        out = readchomp(cmd)
        if !quiet
            println(out)
        end
        if !isempty(out)
            dataTable = Dict{String,Vector{String}}()
            lines = split(out, '\n')
            dashesLine::Int = findfirst(startswith(line, "--") for line in lines)
            if isnothing(dashesLine)
                return dataTable
            end
            ## turn table into dictionary (one key per column)
            headerLine = dashesLine - 1
            dataLines = dashesLine+1:length(lines)
            #@show lines[dashesLine]
            first = 1
            while first < length(lines[dashesLine])
                last = findnext(" ", lines[dashesLine], first)
                if isnothing(last)
                    last = length(lines[dashesLine])
                else
                    last = last[1] + 1
                end
                header = lowercase(strip(lines[headerLine][first:last-1]))
                if header == "job id"
                    header = "jobid"
                end
                if !isempty(header)
                    dataTable[header] = [convert(String, strip(line[first:last-1]))
                                         for line in lines[dataLines]]
                end
                first = last
            end
            ## turn dictionary into array
            jobsData = Dict(dataTable["jobid"][i] =>
                Dict(k => v[i] for (k, v) in dataTable)
                            for i in 1:length(dataLines))
        else
            jobsData::Dict{String,Dict{String,String}} = Dict()
        end
    elseif cluster.jobManager == :Slurm
        if isempty(user)
            cmd = sshCmd(cluster, `squeue`)
        else
            cmd = sshCmd(cluster, `squeue -u $user`)
        end
        out = readchomp(cmd)
        if !quiet
            println(out)
        end
        if !isempty(out)
            dataTable::Dict{String,Vector{String}} = Dict{String,Vector{String}}()
            lines = split(out, '\n')
            ## turn table into dictionary (one key per column)
            headerLine = 1
            dataLines = 2:length(lines)
            first = 1
            while first < length(lines[headerLine])
                last = findnext(r"[^ ] ", lines[headerLine], first)
                if isnothing(last)
                    last = 1000
                else
                    last = last[1] + 1
                end
                header = lowercase(strip(lines[headerLine][first:min(last - 1, end)]))
                if !isempty(header)
                    dataTable[header] = [convert(String, strip(line[first:min(last - 1, end)]))
                                         for line in lines[dataLines]]
                end
                first = last
            end
            ## turn dictionary into array
            jobsData = Dict(dataTable["jobid"][i] =>
                Dict(k => v[i] for (k, v) in dataTable)
                            for i in 1:length(dataLines))
        end
    elseif cluster.jobManager == :ssh
        if isempty(user)
            cmd = sshCmd(cluster, `ps all`)
        else
            cmd = sshCmd(cluster, `ps all -u $user`)
        end
        out = readchomp(cmd)
        if !quiet
            println(out)
        end
        jobsData = Dict()
    else
        error("unknown job manager $(cluster.jobManager)")
    end
    return jobsData
end

"""
    getRunningJob(cluster,jobID;quiet)

Displays/returns specific running/historic job

# Parameters:
+ `cluster::ClusterInfo`: structure with the cluster information
+ `jobID::String`: job id (as returned by `qsub` and understood by `qstat`)
+ `quiet::Bool=false`: display information, rather than just returning it

# Returns:
+ `jobData::Dict{String,String}`: dictionary with data describing the job (fields reported by `qstat
        -f`)
"""
function getRunningJob(
    cluster::ClusterInfo,
    jobID::String;
    quiet=false
)
    jobData = Dict{String,String}()
    if cluster.jobManager == :PBS
        cmd = sshCmd(cluster, `qstat -x -f $jobID`)
        out = readchomp(cmd)
        # remove \n\t from multi-line fields
        out = replace(out, r"\n\t" => "")
        !quiet && println(out)

        if !isempty(out)
            # jobs id
            m = match(r"(?<name>Job Id): (?<value>[-a-z0-9\.]+)\n", out)
            # cleanup value 
            jobData[m[:name]] = m[:value]

            # other fields
            for m in eachmatch(r"\n    (?<name>[^ ]+) = (?<value>.*?)\n    [^ ]"is,
                # i=ignore case, s=. matches new line
                out; overlap=true)
                #display(m)
                value = m[:value]
                value = replace(value, r"<jsdl-hpcpa:[A-Za-z0-9]+>" => "")
                value = replace(value, r"</jsdl-hpcpa:[A-Za-z0-9]+>" => " ")
                jobData[m[:name]] = value
            end

            if haskey(jobData, "executable") && occursin(r"julia", jobData["executable"])
                m = match(r"--project=(?<project>[^ ]+) (?<script>[^ ]+) (?<parameters>.*)",
                    jobData["argument_list"])
                jobData["julia_project"] = m[:project]
                jobData["julia_script"] = m[:script]
                jobData["julia_parameters"] = m[:parameters]
            end
        end
    elseif cluster.jobManager == :Slurm
        cmd = sshCmd(cluster, `scontrol show jobid -dd $jobID`)
        out = readchomp(cmd)
        !quiet && println(out)
        for m in eachmatch(r"(?<name>\S+)=(?<value>\S+)\s+"is,
            # i=ignore case, s=. matches new line
            out; overlap=false)
            #display(m)
            value = m[:value]
            jobData[m[:name]] = value
        end
    else
        error("unknown job manager $(cluster.jobManager)")
    end
    return jobData
end

"""
    getFolderJobs(cluster;wildcard,quiet)

Returns information about all jobs in the servers that already completed.

# Parameters:
+ `cluster::ClusterInfo`: structure with the cluster information
+ `wildcard::String="*"`: wildcard used to select job folders in the server
+ `quiet::Bool=false`: display information, rather than just returning it

# Returns:
+ `jobsData::Vector{Dict{String,String}}`: dictionary with data describing the job (fields reported
        by `qstat -f`)
"""
function getFolderJobs(
    cluster::ClusterInfo;
    wildcard::String="*",
    quiet=false
)
    jobsData::Vector{Dict{String,String}} = Dict{String,String}[]
    errWildcard = joinpath(cluster.jobsFolderRemote, wildcard, "*.iam-pbs.ER")
    cmd = sshCmd(cluster, `ls -1 $errWildcard`) # -1 name get one per line
    (out, err, rc) = conditionalRun(cmd; dryrun=false, quiet=true)
    errFiles = split(out, '\n')
    display(errFiles)
    return errFiles

    # TODO: seems to trigger defense mechanism
    for file in errFiles
        (jobFolderLong, errFile) = splitdir(file)
        (_, jobFolder) = splitdir(jobFolderLong)
        (jobID, ext) = splitext(errFile)
        @printf("Job folder                   = %s\n", jobFolder)
        data = getRunningJob(cluster, jobID; quiet=true)
        if !quiet
            # display(data)
            if haskey(data, "julia_project")
                keys = ["Job Id", "mtime",
                    "executable",
                    "julia_project", "julia_script", "julia_parameters", "resources_used.walltime", "resources_used.ncpus", "resources_used.mem",
                ]
            else
                keys = ["Job Id", "mtime",
                    "executable",
                    #"Submit_arguments",
                    "argument_list", "resources_used.walltime", "resources_used.ncpus", "resources_used.mem",
                ]
            end
            for key in keys
                @printf("   %-25s = %s\n", key, data[key])
            end
        end
        push!(jobsData, data)
    end
    return jobsData
end

"""
    downloadJob(cluster, jobFolder; dryrun, excludeLarge)

Download the contents of one job folder from the cluster using `rsync`.

# Parameters
+ `cluster::ClusterInfo`: structure with the cluster information
+ `jobFolder::String`: folder to retrieve (within `cluster.jobsFolderRemote`)
+ `dryrun::Bool=true`: when `true` commands are not executed, just printed
+ `excludeLarge::Bool=true`: when `true` exclude any files whose name matches '*_large.*'
+ `quiet::Bool=false`: when `true` echos the command
"""
function downloadJob(
    cluster::ClusterInfo,
    jobFolder::String;
    dryrun::Bool=true,
    excludeLarge::Bool=true,
    quiet=false,
)
    jobFolderRemote = joinpath(cluster.jobsFolderRemote, jobFolder)
    if excludeLarge
        cmd = `rsync --prune-empty-dirs  -avz --exclude="*_large.*" $(cluster.username)@$(cluster.ssh2submit):$jobFolderRemote $(cluster.jobsFolderLocal)`
    else
        cmd = `rsync --prune-empty-dirs -avz $(cluster.username)@$(cluster.ssh2submit):$jobFolderRemote $(cluster.jobsFolderLocal)`
    end
    conditionalRun(cmd; quiet, dryrun)
end

"""
    downloadJob(cluster, jobFolder; dryrun, excludeLarge)

Download the contents of all job folders from the cluster using `rsync`.

# Parameters
+ `cluster::ClusterInfo`: structure with the cluster information
+ `dryrun::Bool=true`: when `true` commands are not executed, just printed
+ `excludeLarge::Bool=true`: when `true` exclude any files whose name matches '*_large.*'
+ `quiet::Bool=false`: when `true` echos the command
"""
function downloadAllJobs(
    cluster::ClusterInfo;
    dryrun::Bool=true,
    excludeLarge::Bool=true,
    quiet=false,
)
    jobFolderRemote = cluster.jobsFolderRemote * "/"
    if excludeLarge
        cmd = `rsync -avz --prune-empty-dirs --exclude="*_large.*" $(cluster.username)@$(cluster.ssh2submit):$jobFolderRemote $(cluster.jobsFolderLocal)`
    else
        cmd = `rsync --prune-empty-dirs -avz $(cluster.username)@$(cluster.ssh2submit):$jobFolderRemote $(cluster.jobsFolderLocal)`
    end
    conditionalRun(cmd; quiet, dryrun)
end
