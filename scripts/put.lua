local path = "/v0/entity?id="
local replicas = "&replicas=2/3"
local max=1000000
local id=1

function init()
    indexes = {}
    math.randomseed(os.time())
    --set array of unique indexes
    for i = 1, max do
        indexes[i] = i
    end
    --permutation
    for i = 1, max/2 do
        local j = math.random(i, max)
        indexes[i], indexes[j] = indexes[j], indexes[i]
    end
end

request = function()
    local _id = 0
    if math.random(2) == 2 then
        _id = indexes[id]
        id = (id + 1) % max
    else
        _id = math.random(max)
    end
    return wrk.format("PUT", path .. tostring(_id) .. replicas, nil, randomString(100))
end

function randomString(length)
    local res = ""
    for i = 1, length do
        res = res .. string.char(math.random(97, 122))
    end
    return res
end