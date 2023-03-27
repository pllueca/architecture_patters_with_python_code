import abc
import model
import db_tables as tables
from sqlalchemy import text, insert


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        # Save a batch with its allocations
        stmt = (
            insert(tables.batches)
            .values(
                reference=batch.reference,
                sku=batch.sku,
                _purchased_quantity=batch._purchased_quantity,
                eta=batch.eta,
            )
            .returning(tables.batches.c.id)
        )

        res = self.session.execute(stmt)
        batch_id = res.first().id

        # for each allocation write a new order_line and a new allocation
        for order_line in batch._allocations:
            orderline_res = self.session.execute(
                insert(tables.order_lines)
                .values(
                    orderid=order_line.orderid,
                    sku=order_line.sku,
                    qty=order_line.qty,
                )
                .returning(tables.order_lines.c.id)
            )
            orderline_id = orderline_res.first().id

            self.session.execute(
                insert(tables.allocations).values(
                    orderline_id=orderline_id,
                    batch_id=batch_id,
                )
            )

    def get(self, reference) -> model.Batch:
        # load batch, with its corresponding order_lines

        b = (
            self.session.query(tables.batches)
            .filter(tables.batches.c.reference == reference)
            .one()
        )
        batch_id, batch_ref, batch_sku, batch_qty, batch_eta = b
        order_lines = (
            self.session.query(tables.order_lines)
            .join(tables.allocations)
            .filter(tables.allocations.c.batch_id == batch_id)
            .all()
        )

        batch = model.Batch(batch_ref, batch_sku, batch_qty, batch_eta)
        for _line_id, line_sku, line_qty, line_order_id in order_lines:
            order_line = model.OrderLine(line_order_id, line_sku, line_qty)
            batch.allocate(order_line)

        return batch
