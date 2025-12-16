import { BotConnectorResponse } from '../model/responses';

const INITIAL_RETRY_DELAY = 0;
const RETRY_DELAY_INCREMENT = 1000;
const MAX_RETRY_DELAY = 15000;

enum SseStatus {
  /**
   * The server is not answering, or answering with a 1XX, 3XX, 429, or 5XX HTTP status code
   */
  SERVER_UNAVAILABLE = -1,
  /**
   * The server is answering with a 4XX HTTP status code, except 429 (rate limit)
   */
  UNSUPPORTED = 0,
  /**
   * The server is answering with a 2XX HTTP status code
   */
  SUPPORTED = 1,
}

async function getSseStatus(url: string) {
  try {
    const response = await fetch(url);
    if (response.ok) {
      return SseStatus.SUPPORTED;
    } else if (
      response.status >= 400 &&
      response.status < 500 &&
      response.status !== 429
    ) {
      return SseStatus.UNSUPPORTED;
    } else {
      return SseStatus.SERVER_UNAVAILABLE;
    }
  } catch (_) {
    return SseStatus.SERVER_UNAVAILABLE;
  }
}

export class TockEventSource {
  private initialized: boolean;
  private eventSource: EventSource | null;
  private retryDelay: number;
  private retryTimeoutId: number;
  private pingCheckInterval: number;
  private lastPingTime: number;
  private pingIntervalId: number | undefined;
  onResponse: (botResponse: BotConnectorResponse) => void;
  onStateChange: (state: number) => void;

  constructor(pingCheckInterval: number) {
    this.initialized = false;
    this.retryDelay = INITIAL_RETRY_DELAY;
    this.pingCheckInterval = pingCheckInterval;
  }

  isInitialized(): boolean {
    return this.initialized;
  }

  /**
   * Opens an SSE connection to the given web connector endpoint
   *
   * @param endpoint the base endpoint URL, to which '/sse' will be added to form the full SSE endpoint URL
   * @param userId the locally-generated userId (will be ignored if the backend relies on cookies instead)
   * @returns a promise that gets resolved when the connection is open
   * and gets rejected if the connection fails or this event source is closed
   */
  open(endpoint: string, userId: string): Promise<void> {
    const url = `${endpoint}/sse?userid=${userId}`;
    console.log("Opening SSE connection");
    return this.doOpen(url);
  }

  private doOpen(url: string): Promise<void> {
    this.onStateChange(EventSource.CONNECTING);
    return new Promise<void>((resolve, reject): void => {
      this.tryOpen(url, resolve, reject);
    });
  }

  private tryOpen(url: string, resolve: () => void, reject: () => void) {
    console.log("tryOpen: Starting with url:", url);
    
    if (this.eventSource) {
      console.log("tryOpen: Existing EventSource found this.eventSource:", this.eventSource);
      this.eventSource.close();
      this.eventSource = null;
      console.log("tryOpen: Existing EventSource closed");
    }
    

    if (this.pingIntervalId) {
      console.log("tryOpen: Clearing existing ping interval");
      window.clearInterval(this.pingIntervalId);
      this.pingIntervalId = undefined;
    }
    
    this.eventSource = new EventSource(url);
    console.log("tryOpen: EventSource created");
    this.eventSource.addEventListener('open', () => {
      console.log("SSE connection opened");
      this.lastPingTime = Date.now();
      this.pingIntervalId = window.setInterval(async () => {
        const lastPingTime = this.lastPingTime;
        const pingCheckStartTime = Date.now();
        const timeSinceLastPing = pingCheckStartTime - lastPingTime;
        
        console.log("Checking ping at:", new Date(pingCheckStartTime).toTimeString().substring(0, 12));
        console.log("Time since last ping: ", timeSinceLastPing);
        console.log("Last ping time: ", lastPingTime, "->", new Date(lastPingTime).toTimeString().substring(0, 12));

        if (timeSinceLastPing > this.pingCheckInterval) {
          console.warn("No ping received in the last 30 seconds");
          window.clearInterval(this.pingIntervalId); // Nettoyer le timer actuel
          //this.retry(url, reject, resolve);
          this.close();
          await this.doOpen(url);
        }
      }, this.pingCheckInterval);
      this.onStateChange(EventSource.OPEN);
      this.initialized = true;
      this.retryDelay = INITIAL_RETRY_DELAY;
      resolve();
    });
    this.eventSource.addEventListener('error', (event) => {
      console.log("SSE connection error occurred:", event);
      console.log("EventSource readyState:", this.eventSource?.readyState);
      this.eventSource?.close();
      this.retry(url, reject, resolve);
    });
    this.eventSource.addEventListener('message', (e) => {
      this.onResponse(JSON.parse(e.data));
    });
    this.eventSource.addEventListener('ping', (e) => {
      this.lastPingTime = Date.now();
      console.log("New ping received at ", this.lastPingTime, "->", new Date(this.lastPingTime).toTimeString().substring(0, 12));
    });
  }

  private retry(url: string, reject: () => void, resolve: () => void) {
    console.log("Attempting to retry SSE connection");
    const retryDelay = this.retryDelay;
    this.retryDelay = Math.min(
      MAX_RETRY_DELAY,
      retryDelay + RETRY_DELAY_INCREMENT,
    );
    this.retryTimeoutId = window.setTimeout(async () => {
      switch (await getSseStatus(url)) {
        case SseStatus.UNSUPPORTED:
          console.log("SSE connection unsupported");
          reject();
          this.close();
          break;
        case SseStatus.SUPPORTED:
          console.log("SSE connection supported");
          console.log("Clearing ping interval");
          console.log("About to call tryOpen with url:", url);
          try {
            this.tryOpen(url, resolve, reject);
            console.log("tryOpen called successfully");
          } catch (error) {
            console.error("Error calling tryOpen:", error);
          }
          break;
        case SseStatus.SERVER_UNAVAILABLE:
          console.log("Server unavailable, retrying...");
          this.retry(url, reject, resolve);
          break;
      }
    }, retryDelay);
  }

  close() {
    window.clearTimeout(this.retryTimeoutId);
    window.clearInterval(this.pingIntervalId);
    this.eventSource?.close();
    this.eventSource = null;
    this.initialized = false;
    this.onStateChange(EventSource.CLOSED);
  }
}

