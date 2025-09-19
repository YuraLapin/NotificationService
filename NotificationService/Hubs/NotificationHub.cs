using Microsoft.AspNetCore.SignalR;

namespace NotificationService.Hubs
{
    // <summary>
    // SignalR хаб для websocket соединения
    // </summary>
    public class NotificationHub: Hub<INotificator>
    {
        // <summary>
        // Отправляет в websocket сообщение
        // </summary>
        // <param name="message">
        // Сообщение для отправки
        // </param>
        public async Task Notify(string message)
        {
            await Clients.All.Notify(message);
        }
    }
}
