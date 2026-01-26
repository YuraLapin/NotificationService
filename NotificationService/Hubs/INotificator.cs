namespace NotificationService.Hubs
{
    public interface INotificator
    {
        Task Notify(string message);
    }
}
