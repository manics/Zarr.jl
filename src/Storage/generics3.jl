using AWSCore
using AWSS3

struct GenericS3 <: AbstractStore
    bucket::String
    store::String
    aws::Dict{Symbol, Any}
end


function GenericS3(bucket::String, store::String;
  aws = nothing,
  region = get(ENV, "AWS_DEFAULT_REGION", "us-east-1"),
  creds = nothing,
  )
  if aws === nothing
    aws = aws_config(creds=creds,region=region)
  end
  GenericS3(bucket, store, aws)
end

Base.show(io::IO,s::GenericS3) = print(io,"S3 Object Storage")

# Defined in s3store.ij
# function error_is_ignorable(e)

function Base.getindex(s::GenericS3, i::String)
  try
    return s3_get(s.aws,s.bucket,joinpath(s.store,i))
  catch e
    if error_is_ignorable(e)
      return nothing
    else
      throw(e)
    end
  end
end
getsub(s::GenericS3, d::String) = GenericS3(s.bucket, joinpath(s.store,d), s.aws)

function storagesize(s::GenericS3)
  items = collect(cloud_list_objects(s))
  datafiles = filter(entry -> !any(filename -> endswith(entry["Key"], filename), [".zattrs",".zarray",".zgroup","/"]), items)
  if isempty(datafiles)
    0
  else
    sum(datafiles) do f
      parse(Int, f["Size"])
    end
  end
end

function zname(s::GenericS3)
  d = split(s.store,"/")
  i = findlast(!isempty,d)
  d[i]
end

function isinitialized(s::GenericS3, i::String)
  try
    return s3_exists(s.aws,s.bucket,joinpath(s.store,i))
  catch e
    if error_is_ignorable(e)
      return false
    else
      println(joinpath(s.store,i))
      throw(e)
    end
  end
end

function cloud_list_objects(s::GenericS3)
  prefix = (isempty(s.store) || endswith(s.store,"/")) ? s.store : string(s.store,"/")
  s3_list_objects(s.aws, s.bucket, prefix)
  # s3_list_objects(s.aws, s.bucket, prefix, delimiter="")
end

function subdirs(s::GenericS3)
  items = filter(entry -> endswith(entry["Key"], "/"), collect(cloud_list_objects(s)))
  map(i->i["Key"],items)
end

function Base.keys(s::GenericS3)
  items = cloud_list_objects(s)
  map(i->splitdir(i["Key"])[2],items)
end

path(s::GenericS3) = s.store
