namespace App;

public class ActorMessageEventArgs : EventArgs
{
    public string Payload { get; }
    public string SenderName { get; }

    public ActorMessageEventArgs(string payload, string senderName)
    {
        Payload = payload;
        SenderName = senderName;
    }
}

