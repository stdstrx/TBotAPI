#!/usr/bin/env luajit
local bot = require('TBotAPI')

-- Callback
function bot:on_private_message(msg)
    self:request('sendChatAction', {
        chat_id = msg.chat.id,
        action = 'typing'
    }, true)
    self:request('copyMessage', {
        chat_id = msg.chat.id,
        from_chat_id = msg.from.id,
        message_id = msg.message_id
    }, true)
    return
end

bot.max_connections = 16
bot.debug = false

bot:init('YOUR-BOT-TOKEN-HERE')
while true do
    bot:update(nil, 2)
end
