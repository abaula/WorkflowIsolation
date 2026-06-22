using System.Threading.Channels;

namespace App;

public class SequentialActor : IDisposable, IAsyncDisposable
{
    private readonly Channel<string> _buffer;
    public string Name { get; }

    private readonly CancellationTokenSource _cts;
    private Task? _processingTask;
    private bool _isDisposed;

    // Feedback event for other actors.
    public event EventHandler<ActorMessageEventArgs>? MessageProcessed;

    public SequentialActor(string name)
    {
        Name = name;
        _cts = new CancellationTokenSource();

        // Channel optimization settings for the actor architecture
        _buffer = Channel.CreateUnbounded<string>(new UnboundedChannelOptions
        {
            // FIFO GUARANTEE: Explicitly specify that only ONE worker will read from the buffer.
            // This disables internal read contention and guarantees strict ordering.
            SingleReader = true,

            // Disable synchronous continuations so the calling thread doesn't get dragged
            // into pipeline processing when the buffer is filling up or freeing up.
            AllowSynchronousContinuations = false
        });
    }

    /// <summary>
    /// Non-blocking method for writing a message to the actor's buffer.
    /// Can be safely called concurrently from any other threads.
    /// </summary>
    public ValueTask SendAsync(string message)
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        return _buffer.Writer.WriteAsync(message);
    }

    /// <summary>
    /// Starts the actor's lifecycle. Creates an isolated worker thread.
    /// </summary>
    public void StartProcessing()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);

        if (_processingTask != null)
            return;

        _processingTask = Task.Run(async () =>
        {
            try
            {
                Console.WriteLine($"[{Name}] Started in thread {Environment.CurrentManagedThreadId}");

                // The ReadAllAsync method returns an async stream (IAsyncEnumerable).
                // The loop will consume messages strictly in FIFO order.
                await foreach (var message in _buffer.Reader.ReadAllAsync(_cts.Token))
                {
                    Console.WriteLine($"[{Name}] Received message: '{message}' in thread {Environment.CurrentManagedThreadId}");

                    // Workflow Isolation point: the processing code is executed STRICTLY sequentially.
                    // The next message will not start processing until the current await completes.
                    await ProcessMessageInternalAsync(message);

                    // Notify subscribers via the event
                    OnMessageProcessed(new ActorMessageEventArgs(message, Name));
                }
            }
            catch (OperationCanceledException)
            {
                // Standard graceful interruption via CancellationToken
            }
            catch (Exception ex)
            {
                // Business logic errors should not break the lifecycle of the entire application
                LogError(ex);
            }
        });
    }

    private async Task ProcessMessageInternalAsync(string message)
    {
        Console.WriteLine($"[{Name}] Processing started: {message}");
        // Simulating useful work (database operations, HTTP requests, etc.)
        await Task.Delay(100, _cts.Token);
        Console.WriteLine($"[{Name}] Processing completed: {message}");
    }

    protected virtual void OnMessageProcessed(ActorMessageEventArgs e)
    {
        Console.WriteLine($"[{Name}] Sending message: '{e.Payload}' in thread {Environment.CurrentManagedThreadId}");
        MessageProcessed?.Invoke(this, e);
        Console.WriteLine($"[{Name}] Message sending completed: '{e.Payload}' in thread {Environment.CurrentManagedThreadId}");
    }

    private void LogError(Exception ex) => Console.WriteLine($"[{Name}] Error: {ex.Message}");

    #region Implementation of resource cleanup interfaces (Graceful Shutdown)

    public async ValueTask DisposeAsync()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        // 1. Softly close the buffer for writing.
        // No new messages are accepted, but the ReadAllAsync loop will continue running,
        // until it processes ALL messages already in the queue.
        _buffer.Writer.TryComplete();

        // 2. Wait until the worker thread completely finishes draining the queue
        if (_processingTask != null)
        {
            try { await _processingTask; } catch { }
        }

        MessageProcessed = null; // Prevent memory leaks via events
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        _buffer.Writer.TryComplete();
        _cts.Cancel(); // In a synchronous scenario, we have to abort processing forcefully

        _processingTask?.GetAwaiter().GetResult(); // Safe blocking wait for the Task
        MessageProcessed = null;
        _cts.Dispose();
        GC.SuppressFinalize(this);
    }

    #endregion
}