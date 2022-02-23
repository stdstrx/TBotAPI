local cqueues = require('cqueues')          -- cqueues from luarocks
local http_header = require('http.headers') -- lua_http from luarocks
local http_client = require('http.client')
local http_server = require("http.server")
local http_utils = require("http.util")
local json = require('dkjson')              -- dkjson from luarocks

local api = {
    tagname = 'TBotAPI',
    endpoint = 'api.telegram.org',
    max_connections = 8,
    port = 8998, -- Default Webhook port
    chat_delay = 0.3,
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
function api:request(method, data, queue, __unused__) 
    if queue then
        self._threads:wrap(function () self:request(method, data, nil, true) end)
        return false
    end
    local function streamer()
        local stream
        repeat
            if not self._connection or #self._connection <= 0 then
                self:logd('Creating new %s stream ..', self.max_connections)
                self._connection = {}
                for c = 1, self.max_connections do
                    repeat cqueues.sleep(0.1) until self._connection -- Protect from crash state
                    self._connection[c] = http_client.connect {
                        host = self.endpoint, port = 443, tls = true, version = 1.1
                    }
                end
                self._active_connection = 0
            end
            self._active_connection = self._active_connection >= self.max_connections and 1 or self._active_connection + 1
            stream = self._connection[self._active_connection] ~= nil and self._connection[self._active_connection]:new_stream() or nil
            self._connection[self._active_connection] = stream == nil and nil or self._connection[self._active_connection]
            self:logd('[Connection: %s / %s]: Stream: %s', self._active_connection, #self._connection, stream or 'Invalid')
        until stream ~= nil
        return stream
    end

    self._rque, self._cque = self._rque or 0, self._cque or {}
    self._cque = self._cque or {}
    
    if __unused__ then
        repeat
            cqueues.sleep(0.1)
        until self._rque <= 25
        --self:logd('Processing queue : %s', self._rque)

        if data and data.chat_id then
            self._cque[data.chat_id] = self._cque[data.chat_id] or 0
            self._cque[data.chat_id] = self._cque[data.chat_id] + 1

            local waiting = 0
            if self._cque[data.chat_id] > 1 then
                waiting = self.chat_delay * self._cque[data.chat_id]
                self:logd('chat_id: %s queue : running after %s second ..', data.chat_id, waiting)
            end
            cqueues.sleep(waiting)
            self._cque[data.chat_id] = self._cque[data.chat_id] - 1
        end
    end
    self._rque = self._rque + 1
    
    ::retry::
    local stream, resp
    repeat
        local endpoint = ('/bot%s/%s'):format(self.token, method)
        stream = streamer()

        
        local headers = http_header.new()
        headers:append(':scheme', 'https')
        headers:append(':path', endpoint)
        headers:append(':authority', 'api.telegram.org')
        
        local data = json.encode(data or {})
        headers:append(':method', 'POST')
        headers:append('content-type', 'application/json')
        
        headers:append('content-length', tostring(#data))
        
        self:logd('Connection: %s -> %s', endpoint, data)
        stream:write_headers(headers, false)
        stream:write_body_from_string(data)

        resp = stream:get_body_as_string()
    until resp
    stream:shutdown()

    self._rque = self._rque - 1

    local respJs = json.decode(resp)
    if not respJs then
        return false, resp
    elseif not respJs.ok then
        if respJs.error_code == 429 then
            local retry_after = respJs.parameters.retry_after
            self:log('Too fast, retrying after %s ...', retry_after)
            cqueues.sleep(retry_after)
            goto retry
        end
        self:log('Error: %s [%s] -> %s -> %s', respJs.description, respJs.error_code, api:vardump(respJs), data and json.encode(data) or 'No Data')
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
function api:webhook_update(listen_port, url, allowed_updates, max_connections)
    if not self._hosted then
        if not self:request('setWebhook', { 
            url = url,
            allowed_updates = allowed_updates,
            max_connections = max_connections,
            --drop_pending_updates = true
        }) then error('Failed to use webhook update !') end

        self.update = nil
        self._hosted = http_server.listen({
            host = '0.0.0.0',
            port = listen_port or self.port,
            onstream = function (server, stream)
                local req_headers = assert(stream:get_headers())
                local req = {
                    method = req_headers:get(":method"),
                    path = req_headers:get(":path"),
                    len = req_headers:get("content-length"), 
                    type = req_headers:get('content-type'),
                    content = tostring(stream:get_body_as_string())
                }

                if self.debug then -- Debug request
                    for name, value, never_index in req_headers:each() do
                        self:log('Header: %s -> %s', tostring(name), tostring(value))
                    end
                    if req_method ~= 'GET' then
                        self:log('Request Body -> %s\n', req.content)
                    end
                end

                self._threads:wrap(function () return self:update_parser(json.decode(req.content)) end)
                
                local res_headers = http_header.new()
                res_headers:append(":status", "200")
                assert(stream:write_headers(res_headers, true))
            end,
            onerror = function(server, context, op, err, errno) 
                self:log('%s on %s failed%s', op, context, err and ':' .. err or '')
            end
        })
        assert(self._hosted:listen())
        self:log("Webhook update initialized on port %d",select(3, self._hosted:localname()))
    end


    local stats, err = self._hosted:loop(0.01)
    if not stats then
        self:log('Webhook Thread error: %s', err)
        return stats, err, 'Webhook Update'
    end
    
    local stats, err = self._threads:loop(0.01)
    if not stats then
        self._threads = cqueues.new() -- Re-iniitialized
        self:log('Thread error: %s', err)
        return stats, err, 'Threading'
    end
end
function api:update(limit, timeout, allowed_updates)
    self._offset = self._offset or 0
    self._isProcessing = self._isProcessing or false
    self._crashDetect = self._crashDetect or os.time()

    if not self._delWebhook then
        self._delWebhook = true
        self.webhook_update = nil
        self:request('deleteWebhook')
        self:log('Ready to received update via long polling !')
    end

    local function updater()
        if not self._isProcessing then
            self._isProcessing = true

            self:logd('polling getUpdates | Offset: %s', self._offset)
            local updates = self:request('getUpdates', { offset = self._offset, limit = limit, timeout = timeout, allowed_updates = allowed_updates })
            if updates then
                for idx, resp in pairs(updates.result) do
                    self._offset = resp.update_id + 1
                    self:logd('Processing %s update -> %s', self._offset, self:vardump(resp))
                    self._threads:wrap(function () return self:update_parser(resp) end) 
                end
            end
            
            self._crashDetect = nil
            self._isProcessing = false
        elseif os.time() - self._crashDetect >= 8 then -- Restart connection if it hang for 8 second
            self:log('Crashed, restarting !')
            self._crashDetect = nil; self._isProcessing = nil; self._connection = nil
        end
    end

    if not self._update then
        self._update = true

        self._threads:wrap(updater)
        
        local stats, err = self._threads:loop(0.01)
        if not stats then
            self:log('Thread error: %s', err)
        end

        self._update = false
    end
    
    cqueues.sleep(0.1) -- yield after 100ms
end
function api:init(token, max_connections)
    local _new = setmetatable({
        token = token,
        queue = {},
        queue_done = 0,
        _threads = cqueues.new()
    }, api)
    _new.max_connections = max_connections or _new.max_connections

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
