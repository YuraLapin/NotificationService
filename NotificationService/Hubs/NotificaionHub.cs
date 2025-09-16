using Microsoft.AspNetCore.SignalR;

namespace NotificationService.Hubs
{
    public class NotificaionHub: Hub
    {
        public async Task SendMessage(string message)
        {
            await Clients.All.SendAsync("Send", message);
        }
    }
}
