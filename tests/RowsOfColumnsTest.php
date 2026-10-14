<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

class RowsOfColumnsTest extends \PHPUnit\Framework\TestCase
{
    public function testNegativeColumnIndexIsOutOfBounds(): void
    {
        $rows = new RowsOfColumns([['name', 'email']], 'hash');

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Column index out of bounds: -1');

        $rows->getColumnSelectorsFromHeaderRow([-1]);
    }

    public function testZeroColumnIndexSelectsTheFirstColumn(): void
    {
        $rows = new RowsOfColumns([['name', 'email'], ['Ada', 'ada@example.test']], 'hash');

        self::assertSame([0], $rows->getColumnSelectorsFromHeaderRow([0]));
        self::assertSame([['Ada']], $rows->getRows([0]));
    }
}
