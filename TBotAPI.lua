local cqueues = require('cqueues')          -- cqueues from luarocks
local http_header = require('http.headers') -- lua_http from luarocks
local http_client = require('http.client')
local json = require('dkjson')              -- dkjson from luarocks

local api = {
    tagname = 'TBotAPI',
    endpoint = 'api.telegram.org',
    max_connections = 16,
    debug = false
}
api.__index = api

function api:log(str, ...)
    str = str:gsub('%%[^s^d]', '%%%%') -- Filter invalid %%
    print(os.date('[%d/%m/%Y %H:%M:%S][' .. self.tagname .. '] '), str:format(...))
end
function api:logd(str, ...)
    if self.debug then
        self:log(str, ...)
    end
end
function api:request(method, data, noreturn)
    if noreturn then
        self._threads:wrap(function ()
            self:request(method, data)
        end)
        return false
    end
    local function streamer()
        local stream
        repeat
            local resetThreshold = math.floor((self.max_connections / 100) * 10) -- Create new stream, if alive connection lower than 10%
            if not self._connection or #self._connection < resetThreshold then
                if self.debug then self:log('Creating new stream ..') end
                self._connection = {}
                for c = 1, self.max_connections do
                    self._connection[c] = http_client.connect {
                        host = self.endpoint, port = 443, tls = true, version = 1.1
                    }
                end
                self._active_connection = 0
            end
            self._active_connection = self._active_connection >= self.max_connections and 1 or self._active_connection + 1
            stream = self._connection[self._active_connection] ~= nil and self._connection[self._active_connection]:new_stream() or nil
            self._connection[self._active_connection] = stream == nil and nil or self._connection[self._active_connection]
        until stream ~= nil

        self:logd('Using stream %s to connect', self._active_connection)
        return stream
    end

    local stream, resp
    repeat
        local endpoint = ('/bot%s/%s'):format(self.token, method)
        stream = streamer()
        
        local headers = http_header.new()
        headers:append(':scheme', 'https')
        headers:append(':path', endpoint)
        headers:append(':authority', 'api.telegram.org')
        
        if not data then
            self:logd('Processing %s using GET', endpoint)
            headers:append(':method', 'GET')
            stream:write_headers(headers, true)
        else
            local data = json.encode(data or {})

            self:logd('Processing %s using POST\nData: %s', endpoint, data)
            headers:append(':method', 'POST')
            headers:append('content-type', 'application/json')

            headers:append('content-length', tostring(#data))

            stream:write_headers(headers, false)
            stream:write_body_from_string(data)
        end

        resp = stream:get_body_as_string()
    until resp
    stream:shutdown()

    local respJs = json.decode(resp)
    if not respJs then
        return false, resp
    elseif not respJs.ok then
        self:log('Error: %s [%s] -> %s', respJs.description, respJs.error_code, data and json.encode(data, {indent = true}) or 'No Data')
        return false, respJs
    end
    return respJs, resp
end
function api:update_parser(msg)
    if msg then self:on_update(msg) end

    local callback_name = 'not supported'
    if msg.message then
        if msg.message.chat.type == 'private' then
            self:logd('Callback: on_private_message')
            self:on_private_message(msg.message)
        elseif msg.message.chat.type == 'group' then
            self:logd('Callback: on_group_message')
            self:on_group_message(msg.message)
        elseif msg.message.chat.type == 'supergroup' then
            self:logd('Callback: on_supergroup_message')
            self:on_supergroup_message(msg.message)
        end
        return self:on_message(msg.message)
    elseif msg.edited_message then
        if msg.edited_message.chat.type == 'private' then
            self:logd('Callback: on_edited_private_message')
            self:on_edited_private_message(msg.edited_message)
        elseif msg.edited_message.chat.type == 'group' then
            self:logd('Callback: on_edited_group_message')
            self:on_edited_group_message(msg.edited_message)
        elseif msg.edited_message.chat.type == 'supergroup' then
            self:logd('Callback: on_edited_supergroup_message')
            self:on_edited_supergroup_message(msg.edited_message)
        end
        return self:on_edited_message(msg.edited_message)
    elseif msg.callback_query then
        self:logd('Callback: on_callback_query')
        return self:on_callback_query(msg.callback_query)
    elseif msg.inline_query then
        self:logd('Callback: on_inline_query')
        return self:on_inline_query(msg.inline_query)
    elseif msg.channel_post then
        self:logd('Callback: on_channel_post')
        return self:on_channel_post(msg.channel_post)
    elseif msg.edited_channel_post then
        self:logd('Callback: edited_channel_post')
        return self:on_edited_channel_post(msg.edited_channel_post)
    elseif msg.chosen_inline_result then
        self:logd('Callback: edited_channel_post')
        return self:on_chosen_inline_result(msg.chosen_inline_result)
    elseif msg.shipping_query then
        self:logd('Callback: on_shipping_query')
        return self:on_shipping_query(msg.shipping_query)
    elseif msg.pre_checkout_query then
        self:logd('Callback: pre_checkout_query')
        return self:on_pre_checkout_query(msg.pre_checkout_query)
    elseif msg.poll then
        self:logd('Callback: on_poll')
        return self:on_poll(msg.poll)
    end

    self:log('Failed to parse msg, callback not supported -> %s', json.encode(msg, {indent = true}))
    return false
end
function api:update(limit, timeout, allowed_updates)
    self._offset = self._offset or 0
    self._threads = not self._threads and cqueues.new() or self._threads
    self._isProcessing = self._isProcessing or false
    self._crashDetect = self._crashDetect or os.time()

    local function updater()
        if not self._isProcessing then
            self._isProcessing = true

            self:logd('polling getUpdates | Offset: %s', self._offset)
            local updates = self:request('getUpdates', { offset = self._offset, limit = limit, timeout = timeout, allowed_updates = allowed_updates })
            if updates then
                for idx, resp in pairs(updates.result) do
                    self._offset = resp.update_id + 1
                    --self:logd('Processing %s update -> %s', self._offset, self:vardump(resp))
                    self._threads:wrap(function () return self:update_parser(resp) end)
                end
            end

            self._crashDetect = nil
            self._isProcessing = false
        elseif os.time() - self._crashDetect >= 8 then
            self:log('Crashed, restarting !')
            self._crashDetect = nil; self._isProcessing = nil; self._connection = nil
        end
    end

    self._threads:wrap(updater)
    local stats, err = self._threads:step(0.01)
    if not stats then
        self:log('Thread error: %s', err)
    end

    
    cqueues.sleep(0.00001) -- High CPU Usage hack !
end
function api:init(token)
    local _new = setmetatable({
        token = token
    }, api)

    local resp = _new:request('getMe')
    if not resp then
        _new:log('Failed validating token, please double-check provided token !', token)
        return false
    end

    _new.tagname = resp.result.username
    _new.id = resp.result.id
    _new:log('Initialized with bot id %s with %s connection', _new.id, _new.max_connections)

    return _new
end

function api:vardump(table)
    if not self.debug then
        return ''
    end
    return json.encode(table, { indent = true })
end

do -- Placeholder, replace this callback to get updated !
    function api:on_update(_) end
    function api:on_message(_) end
    function api:on_private_message(_) end
    function api:on_group_message(_) end
    function api:on_supergroup_message(_) end
    function api:on_callback_query(_) end
    function api:on_inline_query(_) end
    function api:on_channel_post(_) end
    function api:on_edited_message(_) end
    function api:on_edited_private_message(_) end
    function api:on_edited_group_message(_) end
    function api:on_edited_supergroup_message(_) end
    function api:on_edited_channel_post(_) end
    function api:on_chosen_inline_result(_) end
    function api:on_shipping_query(_) end
    function api:on_pre_checkout_query(_) end
    function api:on_poll(_) end
    function api:on_poll_answer(_) end
end

return api
