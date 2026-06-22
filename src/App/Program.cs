using App;

internal class Program
{
    private static async Task Main()
    {
        // Create two isolated actors
        await using var orderActor = new SequentialActor("OrderProcessor");
        await using var notificationActor = new SequentialActor("NotificationWorker");

        // SUBSCRIPTION (Observing Workflow Isolation):
        // When OrderActor finishes processing, we notify NotificationActor.
        orderActor.MessageProcessed += (sender, e) =>
        {
            Console.WriteLine($"[{e.SenderName}] Executing MessageProcessed logic in thread {Environment.CurrentManagedThreadId}");
            // Note: this lambda expression is executed in the OrderActor's worker thread!
            // Therefore, there should be no heavy logic here.
            string taskForNotification = $"Send email for order: {e.Payload}";

            // Instantly forward to the neighbor's buffer.
            // The OrderActor's worker thread is freed in fractions of a microsecond.
            _ = notificationActor.SendAsync(taskForNotification);
        };

        // Start independent worker threads for each actor
        orderActor.StartProcessing();
        notificationActor.StartProcessing();

        // Send the initial signal from the external context
        await orderActor.SendAsync("Order #1");
        await orderActor.SendAsync("Order #2");

        // Give the actors time to sequentially execute their tasks.
        await Task.Delay(500);
    }
}