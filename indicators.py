from abc import ABCMeta, abstractmethod


class Indicator(object):
    """
    Indicator is an abstract base class providing an interface for
    all subsequent (inherited) indicator objects.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_next(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_indicator()")


class EMA(Indicator):
    """
    Exponential moving average indicator
    """

    def __init__(self, n):
        self.n = n
        self.last_entry = None
        self.name = 'ema' + str(self.n)

    def calculate_next(self, data_feed: dict):
        #  get newest candle, append ema values to self buffer
        #  do I really need those bufers?

        alpha = 2 / (self.n + 1)

        if self.last_entry:
            ema = alpha * data_feed['close'] + (1 - alpha) * self.last_entry

        else:
            ema = data_feed['close']

        self.last_entry = ema



        





