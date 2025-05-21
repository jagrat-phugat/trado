// Function to get the strike difference for an index
export function getStrikeDiff(indexName: string): number {
  switch (indexName.toLowerCase()) {
    case "nifty":
      return 50;
    case "banknifty":
      return 100;
    case "finnifty":
      return 50;
    case "midcpnifty":
      return 25;
    case "bankex":
      return 100;
    case "sensex":
      return 100;
    default:
      return 100; // Default
  }
}

// Calculate ATM strike based on index LTP
export function getAtmStrike(indexName: string, ltp: number): number {
  const strikeDiff = getStrikeDiff(indexName);
  return Math.round(ltp / strikeDiff) * strikeDiff;
}

// Format option topic for subscription
export function getOptionTopic(indexName: string, tokenNumber: string): string {
  return `NSE_FO|${tokenNumber}`;
}

// Calculate a range of strike prices around ATM strike
export function getStrikeRangeAroundAtm(
  indexName: string,
  atmStrike: number,
  range: number
): number[] {
  const strikeDiff = getStrikeDiff(indexName);
  const strikes: number[] = [];

  // Generate strikes from atmStrike - range*strikeDiff to atmStrike + range*strikeDiff
  for (let i = -range; i <= range; i++) {
    strikes.push(atmStrike + i * strikeDiff);
  }

  return strikes;
}

// Example utility to normalize option types (optional)
export function normalizeOptionType(type: string): "CE" | "PE" | null {
  const upper = type.toUpperCase();
  if (upper === "CE" || upper === "CALL") return "CE";
  if (upper === "PE" || upper === "PUT") return "PE";
  return null;
}