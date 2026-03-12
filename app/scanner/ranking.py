def score(volume_ratio, breakout):
    score = 0
    if breakout:
        score += 50
    score += volume_ratio * 10
    return score