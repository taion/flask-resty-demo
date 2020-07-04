import operator
import uuid

import flask_resty as fr
import flask_sqlalchemy as fsa
import marshmallow as ma
import marshmallow_sqlalchemy as ma_sa
import sqlalchemy as sa
from flask import Flask
from marshmallow import fields
from sqlalchemy import sql
from sqlalchemy.dialects.postgresql import UUID as _UUID

app = Flask(__name__)

app.config.update(
    {
        "SQLALCHEMY_DATABASE_URI": "postgresql://localhost/flask_resty_demo",
        "SQLALCHEMY_TRACK_MODIFICATIONS": False,
    }
)

db = fsa.SQLAlchemy(app)


class UUID(sa.TypeDecorator):
    impl = _UUID(as_uuid=True)


api = fr.Api(app, "/api/v1")


class Widget(db.Model):
    id = sa.Column(UUID, primary_key=True, default=uuid.uuid4)
    serial_number = sa.Column(sa.Text, nullable=False, unique=True)
    created_by = sa.Column(sa.Text, nullable=False)

    sku = sa.Column(sa.Text, nullable=False)
    color = sa.Column(sa.Text, nullable=False)
    size = sa.Column(sa.Integer, nullable=False)

    def assign_color(self, color):
        self.color = f"{color}-assigned"


class WidgetSchema(ma_sa.SQLAlchemySchema):
    class Meta:
        model = Widget

    id = ma_sa.auto_field(required=False)
    serial_number = ma_sa.auto_field()
    created_by = ma_sa.auto_field()

    sku = ma_sa.auto_field()
    color = ma_sa.auto_field()
    size = ma_sa.auto_field()


class HeaderUserAuthentication(fr.HeaderAuthenticationBase):
    def get_credentials_from_token(self, token):
        return {"user_id": token}


class UserAuthorization(
    fr.AuthorizeModifyMixin, fr.HasCredentialsAuthorizationBase
):
    @property
    def request_user_id(self):
        return self.get_request_credentials()["user_id"]

    def filter_query(self, query, view):
        return query.filter(view.model.created_by == self.request_user_id)

    def authorize_modify_item(self, item, action):
        if item.created_by != self.request_user_id:
            raise fr.ApiError(403)


class WidgetViewBase(fr.GenericModelView):
    model = Widget
    schema = WidgetSchema()

    authentication = HeaderUserAuthentication()
    authorization = UserAuthorization()

    filtering = fr.Filtering(
        sku=operator.eq, size_gt=fr.ColumnFilter("size", operator.gt),
    )


class WidgetListView(WidgetViewBase):
    def get(self):
        return self.list()

    def post(self):
        return self.create()


class WidgetView(WidgetViewBase):
    def get(self, id):
        return self.retrieve(id)

    def patch(self, id):
        return self.update(id, partial=True)

    def delete(self, id):
        return self.destroy(id)


api.add_resource("/widgets", WidgetListView, WidgetView, id_rule="<uuid:id>")


class WidgetIncrementSizeView(WidgetViewBase):
    @fr.get_item_or_404(with_for_update=True)
    def put(self, widget):
        self.update_item(widget, {"size": widget.size + 1})
        self.commit()
        return self.make_item_response(widget)


api.add_resource("/widgets/<uuid:id>/increment-size", WidgetIncrementSizeView)


class WidgetColorSchema(ma.Schema):
    id = fields.UUID(required=True)
    color = fields.String(required=True)


class WidgetAssignColorView(WidgetViewBase):
    deserializer = WidgetColorSchema()

    def put(self, id):
        return self.update(id)

    def update_item_raw(self, widget, data):
        widget.assign_color(data["color"])


api.add_resource("/widgets/<uuid:id>/assign-color", WidgetAssignColorView)


class WidgetStatsSchema(ma.Schema):
    created_by = fields.String()
    sku = fields.String()
    num_widgets = fields.Integer()
    total_size = fields.Integer()


class WidgetStatsView(WidgetViewBase):
    schema = WidgetStatsSchema()

    filtering = fr.Filtering(sku=operator.eq)

    @property
    def query_raw(self):
        return self.session.query(
            Widget.created_by,
            Widget.sku,
            sql.func.count().label("num_widgets"),
            sql.func.sum(Widget.size).label("total_size"),
        ).group_by(Widget.created_by, Widget.sku)

    def get(self):
        return self.list()


api.add_resource("/widgets/-/stats", WidgetStatsView)


api.add_ping("/ping")
