package = "lua-resty-async"
version = "dev-1"
source = {
    url = "git://github.com/bungle/lua-resty-async.git"
}
description = {
    summary = "A thread pool based asynchronous job scheduler for OpenResty",
    detailed = "lua-resty-async is a thread pool based job scheduler utilizing Nginx light threads for OpenResty.",
    homepage = "https://github.com/bungle/lua-resty-async",
    maintainer = "Aapo Talvensaari <aapo.talvensaari@gmail.com>",
    license = "BSD"
}
dependencies = {
    "lua >= 5.1"
}
build = {
    type = "builtin",
    modules = {
        ["resty.async"] = "lib/resty/async.lua",
    }
}
