using Microsoft.AspNetCore.SignalR;

namespace NotificationService.Hubs
{
    // <summary>
    // SignalR хаб для websocket соединения
    // </summary>
    public class NotificaionHub: Hub
    {
        // <summary>
        // Отправляет в websocket сообщение
        // </summary>
        // <param name="message">
        // Сообщение для отправки
        // </param>
        public async Task SendMessage(string message)
        {
            await Clients.All.SendAsync("SendMessage", message);
        }
    }
}
