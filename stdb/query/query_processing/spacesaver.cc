#include "spacesaver.h"

namespace stdb {
namespace qp {

static QueryParserToken<SpaceSaver<false>> fi_token("frequent-items");
static QueryParserToken<SpaceSaver<true>>  hh_token("heavy-hitters");

}  // namespace qp
}  // namespace stdb
