class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    pass


class MarketEvent(Event):
    """
    Handles the event of receiving new market data (bars).
    """

    def __init__(self, new_data):
        """
        Initialises the MarketEvent.
        new_data may be a dict or a list of dicts
        """
        self.type = 'MARKET'
        self.new_data = new_data


class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.
    """

    def __init__(self, symbol, datetime, signal: list, last_close_price):
        """
        Initialises the SignalEvent.
        Parameters:
        symbol - The ticker symbol, e.g. 'GOOG'.
        datetime - The timestamp at which the signal was generated.
        signal_type - 'LONG' or 'SHORT'.
        """

        self.type = 'SIGNAL'
        self.symbol = symbol
        self.datetime = datetime
        self.signal = signal
        self.last_close_price = last_close_price


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market or limit),
    quantity and a direction.
    """

    def __init__(self, symbol, datetime, order_type, quantity, last_close_price):
        """
        Initialises the order type, setting whether it is
        a Market order ('MKT') or Limit order ('LMT'), has
        a quantity (integral) and its direction ('BUY' or
        'SELL').

        Parameters:
        symbol - The instrument to trade.
        order_type - 'MKT' or 'LMT' for Market or Limit.
        quantity - Non-negative integer for quantity.
        direction - 'BUY' or 'SELL' for long or short.
        """

        self.type = 'ORDER'
        self.symbol = symbol
        self.datetime = datetime
        self.order_type = order_type
        self.quantity = quantity
        self.last_close_price = last_close_price

    def print_order(self):
        """
        Outputs the values within the Order.
        """
        print(f'Order: type={self.order_type}, symbol={self.symbol}, '
              f'quantity={self.quantity}, price fired={self.last_close_price}')


class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.
    """

    def __init__(self, timeindex, symbol, exchange, quantity, price_filled, commission):
        """
        Initialises the FillEvent object. Sets the symbol, exchange,
        quantity, direction, cost of fill and an optional
        commission.

        If commission is not provided, the Fill object will
        calculate it based on the trade size and Interactive
        Brokers fees.

        Parameters:
        timeindex - The bar-resolution when the order was filled.
        symbol - The instrument which was filled.
        exchange - The exchange where the order was filled.
        quantity - The filled quantity. Positive or negative ('BUY' or 'SELL')
        price_filled - The price at which the order was executed
        commission - An optional commission sent from IB.
        """

        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.price_filled = price_filled
        self.commission = commission
