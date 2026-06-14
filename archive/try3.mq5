
// [Previous property declarations and input parameters remain unchanged]

// Global variables for tick synchronization
datetime g_lastSyncTime = 0;
bool g_initialSyncDone = false;
int g_retryCount = 0;
const int MAX_RETRIES = 3;
ulong g_currentChunkStart = 0;
const ulong CHUNK_SIZE = 2592000000; // 30 days in milliseconds

//+------------------------------------------------------------------+
//| Custom indicator initialization function                           |
//+------------------------------------------------------------------+
int OnInit()
{
    // [Previous initialization code remains unchanged until before g_lastProcessedTickTime initialization]
    
    // Initialize with dummy tick to trigger sync
    MqlTick dummy;
    SymbolInfoTick(_Symbol, dummy);
    
    // Reset sync variables
    g_initialSyncDone = false;
    g_retryCount = 0;
    
    return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Custom indicator iteration function                                |
//+------------------------------------------------------------------+
int OnCalculate(const int rates_total,
                const int prev_calculated,
                const datetime &time[],
                const double &open[],
                const double &high[],
                const double &low[],
                const double &close[],
                const long &tick_volume[],
                const long &volume[],
                const int &spread[])
{
    if(rates_total <= 0)
        return(0);
        
    // Calculate historical data on first run
    if(!g_initialized)
    {
        if(!g_initialSyncDone)
        {
            // Initialize chunk start time
            datetime startTime = TimeCurrent() - InpBacklogMinutes * 60;
            g_currentChunkStart = startTime * 1000; // Convert to milliseconds
            g_initialSyncDone = true;
        }
        
        // Process historical data in chunks
        bool continueProcessing = true;
        MqlTick historical_ticks[];
        int totalBars = 0;
        
        while(continueProcessing)
        {
            ulong chunkEnd = g_currentChunkStart + CHUNK_SIZE;
            if(chunkEnd > TimeCurrent() * 1000)
                chunkEnd = TimeCurrent() * 1000;
                
            int copied = CopyTicksRange(_Symbol, historical_ticks, COPY_TICKS_INFO,
                                      g_currentChunkStart, chunkEnd);
                                      
            if(copied > 0)
            {
                // Reset retry count on successful copy
                g_retryCount = 0;
                
                // Process ticks in this chunk
                MqlRates tempBuffer[];
                
                for(int i = 0; i < copied && totalBars < InpStartingBars; i++)
                {
                    // Filter out irrelevant ticks
                    if((historical_ticks[i].flags & TICK_FLAG_BID) == 0)
                        continue;
                        
                    if(ProcessHistoricalTick(historical_ticks[i], tempBuffer))
                    {
                        totalBars++;
                        
                        if(totalBars >= InpStartingBars)
                        {
                            // Write processed bars to custom symbol
                            if(CustomRatesUpdate(g_customSymbol, tempBuffer))
                            {
                                g_initialized = true;
                                g_lastProcessedTickTime = historical_ticks[i].time_msc;
                                continueProcessing = false;
                                break;
                            }
                        }
                    }
                }
                
                // Move to next chunk if we need more bars
                g_currentChunkStart = chunkEnd;
            }
            else
            {
                int error = GetLastError();
                if(error == ERR_HISTORY_TIMEOUT)
                {
                    if(g_retryCount < MAX_RETRIES)
                    {
                        // Implement exponential backoff
                        Sleep(1000 * (g_retryCount + 1));
                        g_retryCount++;
                        continue;
                    }
                }
                Print("Error copying historical ticks: ", error);
                return(0);
            }
            
            // Check if we've reached current time
            if(chunkEnd >= TimeCurrent() * 1000)
                continueProcessing = false;
        }
    }
    else
    {
        // Process current ticks using chunked approach
        MqlTick current_ticks[];
        ulong fromTime = (g_lastProcessedTickTime - InpTickTolerance * 1000);
        ulong toTime = TimeCurrent() * 1000;
        
        // Split current tick processing into smaller chunks if needed
        while(fromTime < toTime)
        {
            ulong chunkEnd = fromTime + CHUNK_SIZE;
            if(chunkEnd > toTime)
                chunkEnd = toTime;
                
            int copied = CopyTicksRange(_Symbol, current_ticks, COPY_TICKS_INFO,
                                      fromTime, chunkEnd);
                                      
            if(copied > 0)
            {
                for(int i = 0; i < copied; i++)
                {
                    // Filter out irrelevant ticks
                    if((current_ticks[i].flags & TICK_FLAG_BID) == 0)
                        continue;
                        
                    if(current_ticks[i].time_msc > g_lastProcessedTickTime)
                    {
                        ProcessTick(current_ticks[i]);
                        g_lastProcessedTickTime = current_ticks[i].time_msc;
                    }
                }
                fromTime = chunkEnd;
            }
            else
            {
                int error = GetLastError();
                if(error != ERR_HISTORY_TIMEOUT || g_retryCount >= MAX_RETRIES)
                {
                    Print("Error copying current ticks: ", error);
                    break;
                }
                Sleep(1000 * (g_retryCount + 1));
                g_retryCount++;
            }
        }
    }
    
    ChartRedraw(g_customChartID);
    return(rates_total);
}

// [Previous helper functions remain unchanged]

//+------------------------------------------------------------------+
//| Cleanup resources with improved synchronization handling           |
//+------------------------------------------------------------------+
void CleanupResources()
{
    // Reset sync variables
    g_initialSyncDone = false;
    g_retryCount = 0;
    g_currentChunkStart = 0;
    
    // [Previous cleanup code remains unchanged]
}