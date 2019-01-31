# Defines different storages for zarr arrays. Currently only regular files (DirectoryStore)
# and Dictionaries are supported
import JSON

abstract type AbstractStore end

"Normalize logical storage path"
function normalize_path(p::AbstractString)
    p = normpath(p)
    p = replace(p, '\\'=>'/')
    strip(p, '/')
end

# Stores files in a regular file system
struct DirectoryStore <: AbstractStore
    folder::String
    DirectoryStore(p) = new(normalize_path(p))
end
storagesize(d::DirectoryStore) = sum(filter(i->i ∉ (".zattrs",".zarray"),readdir(d.folder))) do f
  filesize(joinpath(d.folder,f))
end

function getattrs(s::DirectoryStore)
    if isfile(joinpath(s.folder, ".zattrs"))
        JSON.parsefile(joinpath(s.folder, ".zattrs"))
    else
        Dict()
    end
end

function getchunk(s::DirectoryStore, i::CartesianIndex)
    f = joinpath(s.folder, join(reverse((i - one(i)).I), '.'))
    if !isfile(f)
        open(f, "w") do _
           nothing
        end
    end
    f
end
isinitialized(s::DirectoryStore, i::CartesianIndex) = isfile(joinpath(s.folder, join(reverse((i - one(i)).I), '.')))

function adddir(s::DirectoryStore, i::String)
    f = joinpath(s.folder, i)
    mkpath(f)
end

zname(s::DirectoryStore) = splitdir(s.folder)[2]


# Stores data in a simple dict in memory
struct DictStore{T} <: AbstractStore
    name::String
    a::T
end
Base.show(io::IO,d::DictStore) = print(io,"Dictionary Storage")

storagesize(d::DictStore) = sum(sizeof,values(d.a))
zname(s::DictStore) = s.name

"Returns the chunk at index i if present"
function getchunk(s::DictStore,  i::CartesianIndex)
    s.a[i]
end

"Checks if a chunk is initialized"
isinitialized(s::DictStore, i::CartesianIndex) = !isempty(s.a[i])
