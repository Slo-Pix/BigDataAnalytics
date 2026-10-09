"""Generate the slide-1 network background (swarms of peers) as SVG + PNG.

Usage: python make_network_bg.py   (needs cairosvg for the PNG step)
"""
import math
import random

W, H = 1920, 1080
NAVY, TEAL, PEER = "#0F172A", "#14B8A6", "#94A3B8"
SEED = 7

random.seed(SEED)


def fade(x):
    """Opacity multiplier: 0 at the left 40% of the slide, 1 at the right edge."""
    t = (x - 0.40 * W) / (0.60 * W)
    t = max(0.0, min(1.0, t))
    return t * t * (3 - 2 * t)  # smoothstep


# Swarms: one hub (tracker/seed) with peers scattered around it
hubs = [(1180, 260), (1520, 180), (1760, 470), (1420, 560), (1650, 850), (1250, 820), (1000, 520)]
nodes, edges = [], []
for hx, hy in hubs:
    hub = len(nodes)
    nodes.append((hx, hy, 9, TEAL))
    for _ in range(random.randint(10, 16)):
        a = random.uniform(0, 2 * math.pi)
        r = random.uniform(60, 190)
        x, y = hx + r * math.cos(a), hy + r * 0.85 * math.sin(a)
        if not (0 < x < W + 40 and -40 < y < H + 40):
            continue
        seeder = random.random() < 0.18
        nodes.append((x, y, random.uniform(2.5, 5) if not seeder else 5.5, TEAL if seeder else PEER))
        edges.append((hub, len(nodes) - 1))

# Peer-to-peer links between nearby peers (any swarm)
for i, (x1, y1, _, _) in enumerate(nodes):
    near = sorted(
        (math.dist((x1, y1), (x2, y2)), j) for j, (x2, y2, _, _) in enumerate(nodes) if j != i
    )
    for d, j in near[:2]:
        if d < 140 and random.random() < 0.6:
            edges.append((i, j))

# A few long links between hubs (DHT / cross-swarm)
hub_ids = [i for i, n in enumerate(nodes) if n[2] == 9]
for a, b in [(0, 1), (1, 2), (2, 3), (3, 0), (3, 4), (4, 5), (5, 6), (6, 0), (3, 5)]:
    edges.append((hub_ids[a], hub_ids[b]))


def build(background):
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">']
    out.append(
        "<defs>"
        f'<radialGradient id="glow"><stop offset="0" stop-color="{TEAL}" stop-opacity="0.45"/>'
        f'<stop offset="1" stop-color="{TEAL}" stop-opacity="0"/></radialGradient>'
        "</defs>"
    )
    if background:
        out.append(f'<rect width="{W}" height="{H}" fill="{NAVY}"/>')
    for i, j in edges:
        x1, y1, r1, _ = nodes[i]
        x2, y2, r2, _ = nodes[j]
        long_link = r1 == 9 and r2 == 9
        op = 0.35 * fade((x1 + x2) / 2) * (0.6 if long_link else 1)
        if op < 0.01:
            continue
        dash = ' stroke-dasharray="6 8"' if long_link else ""
        colour = TEAL if long_link else PEER
        out.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{colour}" '
            f'stroke-width="1.2" stroke-opacity="{op:.3f}"{dash}/>'
        )
    for x, y, r, c in nodes:
        f = fade(x)
        if f < 0.01:
            continue
        if r == 9:
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="42" fill="url(#glow)" opacity="{f:.3f}"/>')
        out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{c}" fill-opacity="{0.85 * f:.3f}"/>')
    out.append("</svg>")
    return "\n".join(out)


if __name__ == "__main__":
    import cairosvg

    for name, bg in [("network_bg", True), ("network_transparent", False)]:
        svg = build(bg)
        with open(f"{name}.svg", "w") as fh:
            fh.write(svg)
        cairosvg.svg2png(bytestring=svg.encode(), write_to=f"{name}.png", output_width=W, output_height=H)
        print("wrote", name + ".svg/.png")
