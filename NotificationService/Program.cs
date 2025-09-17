using NotificationService.Hubs;
using NotificationService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddHostedService<ConsumerService>();
builder.Services.AddSignalR();
builder.Services.AddSingleton<NotificaionHub>();

var app = builder.Build();

app.MapHub<NotificaionHub>("/notifications");

app.Run();
