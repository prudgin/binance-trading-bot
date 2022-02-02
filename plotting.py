import pandas as pd

def plot_buffers(data_handler, ema_strategy):
    prepared = hlp.prepare_df_for_plotting(pd.DataFrame(data_handler.buffered_data))

    plot_df = pd.DataFrame(ema_strategy.buffer)[['open_time',
                                                 ema_strategy.ema_fast.name,
                                                 ema_strategy.ema_slow.name]].copy(deep=True)
    plot_df['open_time'] = pd.to_datetime(plot_df['open_time'], unit='ms')
    plot_df = plot_df.set_index('open_time')
    plot_df = plot_df.astype('float')
    ap = mpf.make_addplot(plot_df[[ema_strategy.ema_fast.name,
                                   ema_strategy.ema_slow.name]])
    mpf.plot(prepared, type='candle', addplot=ap)


